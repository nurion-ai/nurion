"""End-to-end integration tests using real Iceberg catalog and Lance tables"""

import pytest
import pyarrow as pa
import tempfile
import shutil
from pathlib import Path

from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.core.operator import OperatorContext
from solstice.core.models import Record
from solstice.operators.source import IcebergSource, LanceTableSource
from solstice.operators.map import MapOperator, FlatMapOperator
from solstice.operators.batch import MapBatchesOperator
from solstice.operators.filter import FilterOperator
from solstice.operators.sink import FileSink
from solstice.state.backend import LocalStateBackend


@pytest.fixture(scope="module")
def iceberg_catalog():
    """Connect to aether Iceberg REST catalog"""
    try:
        from pyiceberg.catalog import load_catalog
        import os
        
        # Set environment variables for MinIO S3
        os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
        os.environ['AWS_ENDPOINT_URL'] = 'http://localhost:9000'
        os.environ['AWS_REGION'] = 'us-east-1'
        
        catalog = load_catalog(
            "aether",
            **{
                "uri": "http://localhost:8000/api/iceberg-catalog",
                "type": "rest",
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "minioadmin",
                "s3.secret-access-key": "minioadmin",
                "s3.region": "us-east-1",
            }
        )
        return catalog
    except Exception as e:
        pytest.skip(f"Aether catalog not available. Start with: cd ../aether && docker compose up -d. Error: {e}")


@pytest.fixture
def iceberg_test_table(iceberg_catalog):
    """Create test Iceberg table with sample data"""
    # Create namespace
    try:
        iceberg_catalog.create_namespace("test_streaming")
    except:
        pass
    
    table_name = "test_streaming.end_to_end_test"
    
    # Drop if exists
    try:
        iceberg_catalog.drop_table(table_name)
    except:
        pass
    
    # Create schema
    schema = pa.schema([
        ('id', pa.int64()),
        ('value', pa.int64()),
        ('category', pa.string()),
        ('score', pa.float64()),
    ])
    
    # Create table
    table = iceberg_catalog.create_table(table_name, schema=schema)
    
    # Insert test data - diverse dataset for comprehensive testing
    data = pa.table({
        'id': list(range(1, 101)),  # 100 records
        'value': [i * 10 for i in range(1, 101)],
        'category': [f'cat_{i % 5}' for i in range(1, 101)],  # 5 categories
        'score': [0.1 * (i % 10) for i in range(1, 101)],  # Scores 0.1 to 0.9
    })
    
    table.append(data)
    
    yield table_name
    
    # Cleanup
    try:
        iceberg_catalog.drop_table(table_name)
    except:
        pass


@pytest.fixture
def lance_test_table():
    """Create test Lance table"""
    try:
        import lance
    except ImportError:
        pytest.skip("Lance not installed")
    
    tmpdir = tempfile.mkdtemp()
    table_path = Path(tmpdir) / "test_table"
    
    try:
        # Create test data
        data = pa.table({
            'id': list(range(1, 51)),  # 50 records
            'value': [i * 5 for i in range(1, 51)],
            'name': [f'name_{i}' for i in range(1, 51)],
        })
        
        lance.write_dataset(data, str(table_path))
        
        yield str(table_path)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.integration
class TestCompleteE2EPipeline:
    """Complete end-to-end pipeline testing all operators"""
    
    def test_full_pipeline_iceberg_source(self, iceberg_test_table):
        """
        Test complete pipeline with all operators:
        IcebergSource -> MapBatches -> Map -> FlatMap -> Filter -> Sink
        """
        output_dir = tempfile.mkdtemp()
        output_file = Path(output_dir) / "results.json"
        
        try:
            # Define transformations
            def batch_transform(records):
                """Batch-level: Add batch statistics"""
                batch_size = len(records)
                return [
                    Record(
                        key=r.key,
                        value={
                            **r.value,
                            'batch_size': batch_size,
                            'batch_avg': sum(rec.value['value'] for rec in records) / batch_size
                        }
                    )
                    for r in records
                ]
            
            def record_transform(record):
                """Record-level: Add computed field"""
                record['double_value'] = record['value'] * 2
                record['is_high_score'] = record['score'] > 0.5
                return record
            
            def expand_categories(record):
                """FlatMap: Create one record per category attribute"""
                cat = record['category']
                return [
                    {**record, 'cat_type': 'original', 'cat_value': cat},
                    {**record, 'cat_type': 'reversed', 'cat_value': cat[::-1]},
                ]
            
            def filter_high_value(record):
                """Filter: Keep only high value records"""
                return record['value'] > 500
            
            # Create operators
            source = IcebergSource({
                'catalog_uri': 'http://localhost:8000/api/iceberg-catalog',
                'table_name': iceberg_test_table,
                'batch_size': 10,
            })
            
            batch_op = MapBatchesOperator({'map_batches_fn': batch_transform})
            map_op = MapOperator({'map_fn': record_transform})
            flatmap_op = FlatMapOperator({'flatmap_fn': expand_categories})
            filter_op = FilterOperator({'filter_fn': filter_high_value})
            sink = FileSink({
                'output_path': str(output_file),
                'format': 'json',
                'buffer_size': 100,
            })
            
            # Setup contexts
            context = OperatorContext('task1', 'stage1', 'worker1')
            source.open(context)
            batch_op.open(context)
            map_op.open(context)
            flatmap_op.open(context)
            filter_op.open(context)
            sink.open(context)
            
            # Execute pipeline
            records_read = 0
            records_after_batch = 0
            records_after_map = 0
            records_after_flatmap = 0
            records_after_filter = 0
            
            # Process in batches
            from solstice.core.models import Batch
            
            batch_records = []
            for record in source.read():
                records_read += 1
                batch_records.append(record)
                
                if len(batch_records) >= 10:
                    # Process batch
                    batch = Batch(records=batch_records, batch_id=f'batch_{records_read}')
                    batch = batch_op.process_batch(batch)
                    records_after_batch += len(batch.records)
                    
                    # Process each record through pipeline
                    for rec in batch.records:
                        # Map
                        mapped = list(map_op.process(rec))
                        records_after_map += len(mapped)
                        
                        for m_rec in mapped:
                            # FlatMap
                            expanded = list(flatmap_op.process(m_rec))
                            records_after_flatmap += len(expanded)
                            
                            for e_rec in expanded:
                                # Filter
                                filtered = list(filter_op.process(e_rec))
                                records_after_filter += len(filtered)
                                
                                for f_rec in filtered:
                                    # Sink
                                    sink.write(f_rec)
                    
                    batch_records = []
            
            # Process remaining records
            if batch_records:
                batch = Batch(records=batch_records, batch_id='final_batch')
                batch = batch_op.process_batch(batch)
                records_after_batch += len(batch.records)
                
                for rec in batch.records:
                    for m_rec in list(map_op.process(rec)):
                        for e_rec in list(flatmap_op.process(m_rec)):
                            for f_rec in list(filter_op.process(e_rec)):
                                sink.write(f_rec)
            
            # Close all
            sink.close()
            filter_op.close()
            flatmap_op.close()
            map_op.close()
            batch_op.close()
            source.close()
            
            # Verify pipeline execution
            assert records_read == 100, f"Should read 100 records from Iceberg"
            assert records_after_batch == 100, f"Batch op should keep all records"
            assert records_after_map == 100, f"Map op should keep all records"
            assert records_after_flatmap == 200, f"FlatMap should double records (100->200)"
            
            # Filter should keep only value > 500 (records 51-100)
            # After flatmap: 50 records * 2 = 100 records
            assert records_after_filter == 100, f"Filter should keep high value records"
            
            # Verify output file
            assert output_file.exists()
            
            # Read and verify output
            import json
            with open(output_file) as f:
                output_records = [json.loads(line) for line in f]
            
            assert len(output_records) == 100
            
            # Verify transformations applied
            first_record = output_records[0]
            assert 'batch_size' in first_record['value']
            assert 'double_value' in first_record['value']
            assert 'cat_type' in first_record['value']
            assert first_record['value']['value'] > 500
            
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)
    
    def test_lance_to_checkpoint_pipeline(self, lance_test_table):
        """
        Test pipeline with checkpoint and restore:
        LanceSource -> Map -> Filter -> Checkpoint -> Restore -> Continue
        """
        tmpdir = tempfile.mkdtemp()
        checkpoint_dir = Path(tmpdir) / "checkpoints"
        
        try:
            backend = LocalStateBackend(str(checkpoint_dir))
            
            # Define operators
            def add_prefix(record):
                record['name'] = f"processed_{record['name']}"
                return record
            
            def filter_even_ids(record):
                return record['id'] % 2 == 0
            
            # First run: Process first 25 records
            source1 = LanceTableSource({
                'table_path': lance_test_table,
                'batch_size': 5,
            })
            map_op1 = MapOperator({'map_fn': add_prefix})
            filter_op1 = FilterOperator({'filter_fn': filter_even_ids})
            
            context1 = OperatorContext('task1', 'stage1', 'worker1')
            source1.open(context1)
            map_op1.open(context1)
            filter_op1.open(context1)
            
            processed_count = 0
            for i, record in enumerate(source1.read()):
                # Process through pipeline
                for mapped in list(map_op1.process(record)):
                    for filtered in list(filter_op1.process(mapped)):
                        processed_count += 1
                        assert filtered.value['name'].startswith('processed_')
                        assert filtered.value['id'] % 2 == 0
                
                if i >= 24:  # Process 25 records
                    break
            
            # Checkpoint
            checkpoint_state = {
                'source': source1.checkpoint(),
                'map': map_op1.checkpoint(),
                'filter': filter_op1.checkpoint(),
            }
            
            source1.close()
            map_op1.close()
            filter_op1.close()
            
            assert processed_count == 12  # 25 records, 12 even IDs
            assert checkpoint_state['source']['offset'] == 25
            
            # Second run: Restore and process remaining
            source2 = LanceTableSource({
                'table_path': lance_test_table,
                'batch_size': 5,
            })
            map_op2 = MapOperator({'map_fn': add_prefix})
            filter_op2 = FilterOperator({'filter_fn': filter_even_ids})
            
            context2 = OperatorContext('task1', 'stage1', 'worker1')
            source2.open(context2)
            map_op2.open(context2)
            filter_op2.open(context2)
            
            # Restore from checkpoint
            source2.restore(checkpoint_state['source'])
            map_op2.restore(checkpoint_state['map'])
            filter_op2.restore(checkpoint_state['filter'])
            
            # Process remaining records
            remaining_count = 0
            for record in source2.read():
                for mapped in list(map_op2.process(record)):
                    for filtered in list(filter_op2.process(mapped)):
                        remaining_count += 1
            
            source2.close()
            map_op2.close()
            filter_op2.close()
            
            # Should process records 26-50 (25 records, 13 even)
            assert remaining_count == 13
            
            # Total: 12 + 13 = 25 (half of 50 are even)
            
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
    
    def test_batch_processing_optimization(self, lance_test_table):
        """Test MapBatchesOperator for batch-level operations"""
        def batch_statistics(records):
            """Compute batch-level statistics"""
            values = [r.value['value'] for r in records]
            batch_sum = sum(values)
            batch_avg = batch_sum / len(values) if values else 0
            batch_max = max(values) if values else 0
            batch_min = min(values) if values else 0
            
            # Add statistics to each record
            return [
                Record(
                    key=r.key,
                    value={
                        **r.value,
                        'batch_sum': batch_sum,
                        'batch_avg': batch_avg,
                        'batch_max': batch_max,
                        'batch_min': batch_min,
                    }
                )
                for r in records
            ]
        
        source = LanceTableSource({
            'table_path': lance_test_table,
            'batch_size': 10,
        })
        
        batch_op = MapBatchesOperator({'map_batches_fn': batch_statistics})
        
        context = OperatorContext('task1', 'stage1', 'worker1')
        source.open(context)
        batch_op.open(context)
        
        from solstice.core.models import Batch
        
        batch_records = []
        results = []
        
        for record in source.read():
            batch_records.append(record)
            
            if len(batch_records) >= 10:
                batch = Batch(records=batch_records, batch_id='test')
                result_batch = batch_op.process_batch(batch)
                results.extend(result_batch.records)
                
                # Verify batch statistics are added
                for rec in result_batch.records:
                    assert 'batch_sum' in rec.value
                    assert 'batch_avg' in rec.value
                    assert 'batch_max' in rec.value
                    assert 'batch_min' in rec.value
                
                batch_records = []
        
        source.close()
        batch_op.close()
        
        # Should have processed all 50 records
        assert len(results) >= 40
    
    def test_flatmap_scene_detection_pattern(self, lance_test_table):
        """Test FlatMap pattern similar to video scene detection"""
        def detect_scenes(record):
            """Simulate scene detection: one video -> multiple scenes"""
            video_id = record['id']
            num_scenes = (record['value'] // 50) + 1  # Variable scenes per video
            
            scenes = []
            for scene_idx in range(num_scenes):
                scenes.append({
                    'video_id': video_id,
                    'scene_id': scene_idx,
                    'scene_start': scene_idx * 5.0,
                    'scene_end': (scene_idx + 1) * 5.0,
                    'original_value': record['value'],
                })
            return scenes
        
        source = LanceTableSource({
            'table_path': lance_test_table,
            'batch_size': 10,
        })
        
        flatmap = FlatMapOperator({'flatmap_fn': detect_scenes})
        
        context = OperatorContext('task1', 'stage1', 'worker1')
        source.open(context)
        flatmap.open(context)
        
        total_scenes = 0
        videos_processed = 0
        
        for record in source.read():
            scenes = list(flatmap.process(record))
            total_scenes += len(scenes)
            videos_processed += 1
            
            # Verify scene structure
            for scene in scenes:
                assert 'video_id' in scene.value
                assert 'scene_id' in scene.value
                assert 'scene_start' in scene.value
                assert 'scene_end' in scene.value
        
        source.close()
        flatmap.close()
        
        assert videos_processed == 50
        # Different videos produce different number of scenes
        assert total_scenes > videos_processed  # At least some videos have multiple scenes
    
    def test_pipeline_with_state_recovery(self, lance_test_table):
        """Test pipeline with operator state across checkpoints"""
        tmpdir = tempfile.mkdtemp()
        
        try:
            class StatefulCounter(MapOperator):
                """Custom operator that maintains counter state"""
                
                def __init__(self, config):
                    super().__init__(config)
                    self.total_count = 0
                
                def open(self, context):
                    super().open(context)
                    self.total_count = context.get_state('total_count', 0)
                
                def process(self, record):
                    self.total_count += 1
                    self._context.set_state('total_count', self.total_count)
                    
                    record.value['global_position'] = self.total_count
                    return [record]
            
            # First run: Process first 20 records
            source1 = LanceTableSource({'table_path': lance_test_table, 'batch_size': 5})
            counter1 = StatefulCounter({'map_fn': lambda x: x})
            
            ctx1 = OperatorContext('task1', 'stage1', 'worker1')
            source1.open(ctx1)
            counter1.open(ctx1)
            
            for i, record in enumerate(source1.read()):
                results = list(counter1.process(record))
                assert len(results) == 1
                assert results[0].value['global_position'] == i + 1
                
                if i >= 19:
                    break
            
            checkpoint1 = counter1.checkpoint()
            source1.close()
            counter1.close()
            
            assert checkpoint1['total_count'] == 20
            
            # Second run: Restore and continue
            source2 = LanceTableSource({'table_path': lance_test_table, 'batch_size': 5})
            counter2 = StatefulCounter({'map_fn': lambda x: x})
            
            ctx2 = OperatorContext('task1', 'stage1', 'worker1')
            source2.open(ctx2)
            counter2.open(ctx2)
            counter2.restore(checkpoint1)
            
            # Should continue from 21
            assert counter2.total_count == 20
            
            # Process one more record
            for i, record in enumerate(source2.read()):
                if i >= 20:  # Skip first 20 (already processed)
                    results = list(counter2.process(record))
                    assert results[0].value['global_position'] == 21
                    break
            
            source2.close()
            counter2.close()
            
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.integration  
class TestIcebergCatalogIntegration:
    """Integration tests using real aether Iceberg REST catalog"""
    
    def test_iceberg_source_read_and_checkpoint(self, iceberg_test_table):
        """Test IcebergSource reading from aether catalog"""
        source = IcebergSource({
            'catalog_uri': 'http://localhost:8000/api/iceberg-catalog',
            'table_name': iceberg_test_table,
            'batch_size': 20,
        })
        
        context = OperatorContext('task1', 'stage1', 'worker1')
        source.open(context)
        
        # Read first 30 records
        records = []
        for i, record in enumerate(source.read()):
            records.append(record)
            if i >= 29:
                break
        
        assert len(records) == 30
        assert records[0].value['id'] == 1
        assert records[29].value['id'] == 30
        
        # Checkpoint
        checkpoint = source.checkpoint()
        assert checkpoint['offset'] == 30
        
        source.close()
        
        # Restore and continue
        source2 = IcebergSource({
            'catalog_uri': 'http://localhost:8000/api/iceberg-catalog',
            'table_name': iceberg_test_table,
            'batch_size': 20,
        })
        
        context2 = OperatorContext('task1', 'stage1', 'worker1')
        source2.open(context2)
        source2.restore(checkpoint)
        
        # Should start from record 31
        remaining = list(source2.read())
        assert len(remaining) == 70  # 100 total - 30 already processed
        assert remaining[0].value['id'] == 31
        
        source2.close()


# Mark module
pytestmark = pytest.mark.integration

