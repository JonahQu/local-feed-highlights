#!/usr/bin/env python3
"""Bounded local summary workflow. Uses the existing feed, never controls WeChat."""
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
from zoneinfo import ZoneInfo

PROJECT = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.environ.get('HIGHLIGHTS_CONFIG', str(PROJECT / 'config.json')))
CONFIG = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
ROOT = Path(os.environ.get('HIGHLIGHTS_ROOT', str(PROJECT / 'runtime'))).resolve()
FEED = CONFIG.get('feed', '')
TZ = ZoneInfo(CONFIG.get('timezone', 'UTC'))


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def load(name, default=None):
    p = ROOT / name
    return json.loads(p.read_text()) if p.exists() else default


def save(name, obj):
    p = ROOT / name
    p.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = p.with_name(p.name + '.tmp')
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + '\n')
    os.chmod(tmp, 0o600)
    tmp.replace(p)


def feed(*args):
    if not FEED or not CONFIG.get('account_tag'):
        raise ValueError('configure_feed_and_account_tag_first')
    result = subprocess.run([FEED, *args], capture_output=True, text=True, timeout=90)
    if result.returncode:
        raise RuntimeError('existing_feed_failed')
    return json.loads(result.stdout)


def health(s):
    return {k: s.get(k) for k in ('account_tag', 'collection_state', 'collection_stale',
        'fault_present', 'archive_captured_at_utc', 'last_success_age_seconds', 'login_state')}


def publish(items, note, source_health, batch=None):
    previous = load('latest.json', {})
    doc = {'schema': 'wechat-highlights-v1', 'version': previous.get('version', 0) + 1,
           'updated_at_utc': now(), 'timezone': str(TZ),
           'source': 'existing_primary_wechat_archive', 'source_health': source_health,
           'highlights': items, 'collection_note': note, 'drafts_only': True,
           'coverage': 'local synchronized records; incomplete history and nontext recognition',
           'dot_readback_verified': False}
    if batch:
        doc['batch_id'] = batch['batch_id']
        doc['through_seq'] = batch['through_seq']
        for key in ('after_seq', 'message_range', 'metadata_events', 'body_read_seqs', 'deferred_identity_seqs', 'remaining_after_batch', 'observed_through_seq'):
            if key in batch:
                doc[key] = batch[key]
        doc['processing_error'] = None
    save('history/' + str(doc['version']).zfill(8) + '.json', doc)
    save('latest.json', doc)
    return doc


def checked_batch(r):
    b = r['batch']
    digest = hashlib.sha256(json.dumps(b, ensure_ascii=False, sort_keys=True,
        separators=(',', ':')).encode()).hexdigest()
    if digest != r['payload_sha256']:
        raise RuntimeError('batch_hash_mismatch_no_ack')
    return b


def ack(meta):
    return feed('ack', '--consumer', 'codex-hourly-highlights', '--batch-id',
                meta['batch_id'], '--sha256', meta['sha256'])


def prepare():
    clock = dt.datetime.now(TZ)
    if not (6 <= clock.hour <= 23 or (clock.hour == 0 and clock.minute < 15)):
        return {'action': 'quiet_outside_daytime'}
    state = load('state.json', {})
    if 'start_message_time' not in state:
        raise ValueError('run_init_first')
    s = feed('status', '--consumer', 'codex-hourly-highlights')
    h = health(s)
    save('health.json', {'checked_at_utc': now(), **h})
    if s.get('account_tag') != CONFIG.get('account_tag'):
        raise RuntimeError('account_scope_changed_no_read')
    closing_pause = clock.hour == 0 and s.get('collection_state') == 'scheduled_pause' and not s.get('fault_present')
    if not closing_pause and (s.get('collection_stale') or s.get('fault_present') or s.get('collection_state') != 'running'):
        key = [s.get('collection_state'), s.get('fault_present'), s.get('archive_captured_at_utc')]
        changed = key != state.get('reported_fault')
        if changed:
            doc = publish([], '采集已中断，当前不能将无新增视为没有重要消息。保留保护校验，不自动清理 FAULT 或重新取钥。', h)
            state['reported_fault'] = key
            save('state.json', state)
            return {'action': 'report_fault', 'version': doc['version'], 'health': h}
        return {'action': 'quiet_known_fault', 'health': h}
    if state.get('reported_fault'):
        doc = publish([], '源采集已恢复读取；不据此推断服务器在线或历史完整。', h)
        state.pop('reported_fault', None)
        save('state.json', state)
        return {'action': 'report_recovery', 'version': doc['version'], 'health': h}
    committed = load('committed-batch.json')
    pending = load('pending.json')
    if pending and committed and committed.get('batch_id') == pending['batch_id']:
        ack(pending)
        (ROOT / 'pending.json').unlink()
    if not s.get('has_changes'):
        return {'action': 'quiet_no_change', 'health': h}
    for _ in range(10):
        r = feed('read', '--consumer', 'codex-hourly-highlights', '--limit', '100',
                 '--max-chars', '6000', '--per-message-chars', '1200', '--metadata-only')
        if not r.get('has_changes'):
            return {'action': 'quiet_baseline_complete', 'health': h}
        b = checked_batch(r)
        meta = {'batch_id': b['batch_id'], 'sha256': r['payload_sha256'],
                'through_seq': b['through_seq'], 'after_seq': b['after_seq']}
        events = [e for e in b['events'] if e['message_time'] >= state['start_message_time']]
        if not events:
            # The user requested future monitoring: record historical-baseline coverage before ack.
            save('baseline.json', {**meta, 'recorded_at_utc': now(), 'reason': 'pre_request_history'})
            ack(meta)
            continue
        # Only mappings already explicit in prior work summaries; unknown identities remain metadata.
        work = CONFIG.get('conversations', {})
        selected = []
        for event in events:
            if event['conversation'] in work and event.get('has_text'):
                detail = feed('get', '--seq', str(event['seq']), '--max-chars', '1200')
                if detail.get('conversation') != event['conversation'] or detail.get('message_time') != event['message_time']:
                    raise RuntimeError('event_metadata_mismatch_no_ack')
                selected.append(detail)
        metadata = [{k: e.get(k) for k in ('seq', 'conversation', 'message_time', 'captured_at_utc', 'message_type', 'has_text', 'text_chars')} for e in events]
        times = [e['message_time'] for e in events if isinstance(e.get('message_time'), int)]
        body_seqs = [e['seq'] for e in selected]
        extra = {'message_range': {'start': min(times) if times else None, 'end': max(times) if times else None, 'timezone': str(TZ)},
                 'metadata_events': metadata, 'body_read_seqs': body_seqs,
                 'deferred_identity_seqs': [e['seq'] for e in events if e.get('has_text') and e['conversation'] not in work],
                 'remaining_after_batch': r.get('more_events', 0), 'observed_through_seq': r.get('observed_through_seq')}
        save('pending.json', {**meta, **extra, 'source_seqs': [e['seq'] for e in selected], 'health': h})
        return {'action': 'summarize', 'batch': meta, **extra,
                'more_events': r.get('more_events', 0), 'health': h,
                'previous_highlights': load('latest.json', {}).get('highlights', []),
                'collection_note_required': 'Metadata coverage is not body review. Unmapped identities remain deferred by source seq; no inference of no work.',
                'events': selected}
    return {'action': 'quiet_baseline_backlog', 'health': h}


def commit(path):
    p = load('pending.json')
    summary = json.loads(Path(path).read_text())
    if summary.get('batch_id') != p['batch_id']:
        raise ValueError('wrong_batch_no_ack')
    items = summary.get('highlights')
    if not isinstance(items, list) or len(items) > 5:
        raise ValueError('need_at_most_five_highlights')
    if len(json.dumps(items, ensure_ascii=False)) > 5000:
        raise ValueError('summary_too_long')
    valid = set(p['source_seqs'])
    for item in items:
        if not item.get('source_seqs') or not set(item['source_seqs']).issubset(valid):
            raise ValueError('invalid_source_reference_no_ack')
    if p.get('deferred_identity_seqs') and not summary.get('collection_note'):
        raise ValueError('deferred_identity_coverage_note_required_no_ack')
    old = load('latest.json', {})
    if old.get('batch_id') == p['batch_id']:
        doc = old
    else:
        doc = publish(items, summary.get('collection_note', ''), p['health'], p)
    # Durable summary and commit receipt precede source acknowledgement.
    save('committed-batch.json', p)
    ack(p)
    (ROOT / 'pending.json').unlink()
    state = load('state.json', {})
    state.pop('reported_fault', None)
    save('state.json', state)
    return {'committed': True, 'version': doc['version'], 'highlight_count': len(items)}


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['init', 'prepare', 'commit', 'read', 'status'])
    parser.add_argument('--input')
    parser.add_argument('--since-version', type=int, default=-1)
    parser.add_argument('--after-version', type=int)
    parser.add_argument('--limit', type=int, default=5)
    args = parser.parse_args()
    ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    if args.action == 'init':
        if load('state.json') is not None:
            raise ValueError('already_initialized')
        save('state.json', {'start_message_time': CONFIG.get('start_message_time', int(dt.datetime.now(dt.timezone.utc).timestamp()))})
        print(json.dumps({'initialized': True}))
        return
    if args.action == 'read' and args.after_version is not None:
        if not 1 <= args.limit <= 24:
            raise ValueError('limit_must_be_1_to_24')
        files = sorted(p for p in (ROOT / 'history').glob('*.json')
                       if int(p.stem) > args.after_version)
        docs = [json.loads(p.read_text()) for p in files[:args.limit]]
        result = {'schema': 'wechat-highlights-history-v1', 'summaries': docs,
                  'next_version': docs[-1]['version'] if docs else args.after_version,
                  'more_available': len(files) > len(docs), 'health': load('health.json', {}),
                  'dot_readback_verified': False}
    elif args.action == 'read':
        doc = load('latest.json', {})
        result = ({'unchanged': True, 'version': doc.get('version'), 'health': load('health.json', {})}
                  if doc.get('version', 0) <= args.since_version else {**doc, 'last_check': load('health.json', {})})
    elif args.action == 'status':
        doc = load('latest.json', {})
        result = {'version': doc.get('version', 0), 'updated_at_utc': doc.get('updated_at_utc'),
                  'health': load('health.json', {}), 'dot_readback_verified': False}
    else:
        with (ROOT / 'workflow.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = prepare() if args.action == 'prepare' else commit(args.input)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
