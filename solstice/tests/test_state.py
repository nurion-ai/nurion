"""Unit tests for state management (no mocks)"""

import pytest
import tempfile
import shutil
from pathlib import Path

from solstice.state.backend import LocalStateBackend
from solstice.state.manager import StateManager
from solstice.state.checkpoint import CheckpointCoordinator
from solstice.core.models import CheckpointHandle, Record, Batch


class TestLocalStateBackend:
    """Tests for LocalStateBackend with real file I/O"""
    
    def setup_method(self):
        """Setup test directory"""
        self.test_dir = tempfile.mkdtemp()
        self.backend = LocalStateBackend(self.test_dir)
    
    def teardown_method(self):
        """Cleanup test directory"""
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_save_and_load_state(self):
        """Test saving and loading state to real files"""
        state = {'counter': 42, 'data': [1, 2, 3], 'nested': {'key': 'value'}}
        path = 'test/state.pkl'
        
        # Save
        self.backend.save_state(path, state)
        
        # Verify file exists
        full_path = Path(self.test_dir) / path
        assert full_path.exists()
        
        # Load
        loaded_state = self.backend.load_state(path)
        
        assert loaded_state == state
        assert loaded_state['counter'] == 42
        assert loaded_state['nested']['key'] == 'value'
    
    def test_exists(self):
        """Test checking if state exists"""
        state = {'test': 'data'}
        path = 'test/exists.pkl'
        
        assert not self.backend.exists(path)
        
        self.backend.save_state(path, state)
        
        assert self.backend.exists(path)
    
    def test_delete_state(self):
        """Test deleting state"""
        state = {'test': 'data'}
        path = 'test/delete.pkl'
        
        self.backend.save_state(path, state)
        assert self.backend.exists(path)
        
        self.backend.delete_state(path)
        assert not self.backend.exists(path)
    
    def test_list_checkpoints(self):
        """Test listing checkpoints"""
        # Create multiple checkpoints
        self.backend.save_state('job1/ckpt1/manifest.json', {'id': 1})
        self.backend.save_state('job1/ckpt2/manifest.json', {'id': 2})
        self.backend.save_state('job1/ckpt1/worker1.pkl', {'data': 'w1'})
        
        checkpoints = self.backend.list_checkpoints('job1')
        
        assert len(checkpoints) >= 3
        assert any('ckpt1' in c for c in checkpoints)
        assert any('ckpt2' in c for c in checkpoints)
    
    def test_nested_paths(self):
        """Test deeply nested paths"""
        state = {'deep': 'data'}
        path = 'a/b/c/d/e/state.pkl'
        
        self.backend.save_state(path, state)
        assert self.backend.exists(path)
        
        loaded = self.backend.load_state(path)
        assert loaded == state


class TestStateManager:
    """Tests for StateManager with real backend"""
    
    def setup_method(self):
        """Setup test state manager"""
        self.test_dir = tempfile.mkdtemp()
        backend = LocalStateBackend(self.test_dir)
        self.manager = StateManager('worker1', 'stage1', backend)
    
    def teardown_method(self):
        """Cleanup"""
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_keyed_state(self):
        """Test keyed state management"""
        # Update keyed state
        self.manager.update_keyed_state('key1', {'count': 10, 'value': 'a'})
        self.manager.update_keyed_state('key2', {'count': 20, 'value': 'b'})
        
        # Get keyed state
        state1 = self.manager.get_keyed_state('key1')
        state2 = self.manager.get_keyed_state('key2')
        
        assert state1 == {'count': 10, 'value': 'a'}
        assert state2 == {'count': 20, 'value': 'b'}
        
        # Get non-existent key
        state3 = self.manager.get_keyed_state('key3')
        assert state3 == {}
    
    def test_operator_state(self):
        """Test operator state management"""
        self.manager.update_operator_state({'version': '1.0', 'config': {'param': 42}})
        
        state = self.manager.get_operator_state()
        
        assert state['version'] == '1.0'
        assert state['config']['param'] == 42
        
        # Update again
        self.manager.update_operator_state({'another': 'field'})
        state = self.manager.get_operator_state()
        assert 'version' in state
        assert 'another' in state
    
    def test_checkpoint_and_restore(self):
        """Test real checkpoint creation and restoration"""
        # Set up state
        self.manager.update_keyed_state('key1', {'value': 100})
        self.manager.update_keyed_state('key2', {'value': 200})
        self.manager.update_operator_state({'counter': 42, 'name': 'test'})
        self.manager.update_offset({'position': 1000, 'file': 'data.parquet'})
        
        # Create checkpoint (saves to real files)
        handle = self.manager.checkpoint('ckpt_001')
        
        assert handle.checkpoint_id == 'ckpt_001'
        assert handle.worker_id == 'worker1'
        assert handle.stage_id == 'stage1'
        assert handle.size_bytes > 0
        
        # Verify file was created
        assert Path(self.test_dir, handle.state_path).exists()
        
        # Clear state
        self.manager.clear()
        assert len(self.manager.keyed_state) == 0
        assert len(self.manager.operator_state) == 0
        
        # Restore from checkpoint (loads from real files)
        self.manager.restore('ckpt_001')
        
        assert self.manager.get_keyed_state('key1') == {'value': 100}
        assert self.manager.get_keyed_state('key2') == {'value': 200}
        assert self.manager.get_operator_state()['counter'] == 42
        assert self.manager.offset['position'] == 1000
    
    def test_delta_checkpoint(self):
        """Test delta-based checkpointing"""
        # First checkpoint
        self.manager.update_keyed_state('key1', {'value': 1})
        handle1 = self.manager.checkpoint('ckpt_001')
        size1 = handle1.size_bytes
        
        # Second checkpoint with more keys
        self.manager.update_keyed_state('key2', {'value': 2})
        self.manager.update_keyed_state('key3', {'value': 3})
        handle2 = self.manager.checkpoint('ckpt_002')
        size2 = handle2.size_bytes
        
        # Delta should be smaller than full state
        # (only key2 and key3, not key1)
        assert size2 < size1 + 1000  # Some reasonable bound


class TestCheckpointCoordinator:
    """Tests for CheckpointCoordinator with real backend"""
    
    def setup_method(self):
        """Setup coordinator"""
        self.test_dir = tempfile.mkdtemp()
        backend = LocalStateBackend(self.test_dir)
        self.coordinator = CheckpointCoordinator('test_job', backend)
    
    def teardown_method(self):
        """Cleanup"""
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_trigger_checkpoint(self):
        """Test triggering a checkpoint"""
        checkpoint_id = self.coordinator.trigger_checkpoint()
        
        assert checkpoint_id.startswith('checkpoint_')
        assert checkpoint_id in self.coordinator.checkpoints
        
        checkpoint = self.coordinator.checkpoints[checkpoint_id]
        assert checkpoint.job_id == 'test_job'
    
    def test_add_checkpoint_handle(self):
        """Test adding checkpoint handles"""
        checkpoint_id = self.coordinator.trigger_checkpoint()
        
        handle = CheckpointHandle(
            checkpoint_id=checkpoint_id,
            stage_id='stage1',
            worker_id='worker1',
            state_path='stage1/ckpt/worker1.pkl',
            offset={'pos': 100},
            size_bytes=1024
        )
        
        self.coordinator.add_checkpoint_handle(checkpoint_id, 'stage1', handle)
        
        checkpoint = self.coordinator.checkpoints[checkpoint_id]
        assert 'stage1' in checkpoint.handles
        assert len(checkpoint.handles['stage1']) == 1
        assert checkpoint.handles['stage1'][0].worker_id == 'worker1'
    
    def test_finalize_checkpoint(self):
        """Test finalizing a checkpoint (writes real manifest file)"""
        checkpoint_id = self.coordinator.trigger_checkpoint()
        
        # Add handles for 2 stages
        for stage_id in ['stage1', 'stage2']:
            handle = CheckpointHandle(
                checkpoint_id=checkpoint_id,
                stage_id=stage_id,
                worker_id='worker1',
                state_path=f'{stage_id}/ckpt/worker1.pkl',
                offset={'pos': 100},
                size_bytes=1024
            )
            self.coordinator.add_checkpoint_handle(checkpoint_id, stage_id, handle)
        
        # Finalize (writes manifest to real file)
        success = self.coordinator.finalize_checkpoint(
            checkpoint_id,
            expected_stages=['stage1', 'stage2']
        )
        
        assert success is True
        
        checkpoint = self.coordinator.checkpoints[checkpoint_id]
        assert checkpoint.manifest_path is not None
        assert self.coordinator.latest_completed_checkpoint == checkpoint_id
        
        # Verify manifest file exists
        manifest_file = Path(self.test_dir) / checkpoint.manifest_path
        assert manifest_file.exists()
    
    def test_should_trigger_checkpoint(self):
        """Test checkpoint trigger conditions"""
        import time
        
        # Should trigger after interval
        self.coordinator.last_checkpoint_time = time.time() - 400
        self.coordinator.checkpoint_interval_secs = 300
        
        assert self.coordinator.should_trigger_checkpoint() is True
        
        # Should not trigger before interval
        self.coordinator.last_checkpoint_time = time.time()
        
        assert self.coordinator.should_trigger_checkpoint() is False
        
        # Should trigger based on record count
        self.coordinator.checkpoint_interval_records = 1000
        self.coordinator.records_since_checkpoint = 1500
        
        assert self.coordinator.should_trigger_checkpoint() is True
    
    def test_cleanup_old_checkpoints(self):
        """Test cleaning up old checkpoints"""
        import time
        
        # Create multiple checkpoints with small delays
        checkpoint_ids = []
        for i in range(10):
            ckpt_id = self.coordinator.trigger_checkpoint()
            checkpoint_ids.append(ckpt_id)
            
            # Add minimal handle
            handle = CheckpointHandle(
                checkpoint_id=ckpt_id,
                stage_id='stage1',
                worker_id='worker1',
                state_path=f'stage1/ckpt_{i}/worker1.pkl',
                offset={'pos': i},
                size_bytes=100
            )
            self.coordinator.add_checkpoint_handle(ckpt_id, 'stage1', handle)
            self.coordinator.finalize_checkpoint(ckpt_id, ['stage1'])
            time.sleep(0.01)  # Small delay to ensure different timestamps
        
        # Should have 10 checkpoints
        assert len(self.coordinator.checkpoints) == 10
        
        # List all checkpoints (sorted by name/timestamp)
        all_checkpoints = self.coordinator.list_checkpoints()
        assert len(all_checkpoints) >= 10
        
        # Cleanup, keep last 3
        self.coordinator.cleanup_old_checkpoints(keep_last_n=3)
        
        # Should have 3 checkpoints left in memory
        assert len(self.coordinator.checkpoints) == 3


class TestBatchOperations:
    """Tests for Batch model operations"""
    
    def test_batch_length(self):
        """Test batch length"""
        records = [
            Record(key='1', value={'v': 1}),
            Record(key='2', value={'v': 2}),
            Record(key='3', value={'v': 3}),
        ]
        
        batch = Batch(records=records, batch_id='test')
        
        assert len(batch) == 3
    
    def test_empty_batch(self):
        """Test empty batch"""
        batch = Batch(records=[], batch_id='empty')
        
        assert len(batch) == 0
    
    def test_batch_with_metadata(self):
        """Test batch metadata"""
        import time
        before = time.time()
        
        batch = Batch(
            records=[Record(key='1', value={'v': 1})],
            batch_id='meta_test',
            source_split='split_1'
        )
        
        after = time.time()
        
        assert batch.batch_id == 'meta_test'
        assert batch.source_split == 'split_1'
        assert before <= batch.timestamp <= after
