import sys,unittest,tempfile,json
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import highlights as h
class HighlightsTests(unittest.TestCase):
    def test_bad_hash_rejected(self):
        with self.assertRaises(RuntimeError):h.checked_batch({'batch':{},'payload_sha256':'bad'})
    def test_commit_before_ack_and_retry_same_version(self):
        with tempfile.TemporaryDirectory() as d,patch.object(h,'ROOT',Path(d)):
            h.save('state.json',{'start_message_time':0})
            h.save('pending.json',{'batch_id':'demo','sha256':'abc','through_seq':1,'source_seqs':[1],'health':{}})
            path=Path(d)/'summary.json';path.write_text(json.dumps({'batch_id':'demo','highlights':[{'source_seqs':[1],'fact':'Synthetic example'}]}))
            with patch.object(h,'ack',side_effect=RuntimeError('temporary feed failure')):
                with self.assertRaises(RuntimeError):h.commit(path)
            self.assertEqual(h.load('latest.json')['version'],1)
            self.assertTrue((Path(d)/'pending.json').exists())
            with patch.object(h,'ack',return_value={'ok':True}):h.commit(path)
            self.assertEqual(h.load('latest.json')['version'],1)
            self.assertFalse((Path(d)/'pending.json').exists())
    def test_invalid_reference_never_acknowledged(self):
        with tempfile.TemporaryDirectory() as d,patch.object(h,'ROOT',Path(d)),patch.object(h,'ack') as ack:
            h.save('pending.json',{'batch_id':'demo','source_seqs':[1]})
            path=Path(d)/'summary.json';path.write_text(json.dumps({'batch_id':'demo','highlights':[{'source_seqs':[99]}]}))
            with self.assertRaises(ValueError):h.commit(path)
            ack.assert_not_called()
if __name__=='__main__':unittest.main()
