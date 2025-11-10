#!/usr/bin/env python3
"""
Quickstart example for Solstice Streaming

This is a minimal example that demonstrates the core concepts:
1. Creating a job
2. Defining stages with operators
3. Building a DAG
4. Running with checkpoints
"""

import logging
import time
from typing import Any, Dict

import ray

from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.core.operator import SourceOperator, Operator, SinkOperator
from solstice.core.models import Record
from solstice.state.backend import LocalStateBackend


# 1. Define custom operators
class NumberSource(SourceOperator):
    """Source that generates numbers 1 to N"""
    
    def open(self, context):
        super().open(context)
        self.max_num = self.config.get('max_num', 100)
        self.current = self._context.get_state('current', 1)
        print(f"NumberSource: Starting from {self.current}")
    
    def read(self):
        """Generate numbers"""
        while self.current <= self.max_num:
            yield Record(
                key=str(self.current),
                value={'number': self.current}
            )
            self.current += 1
            
            # Update state for checkpointing
            self._context.set_state('current', self.current)
            
            # Simulate some processing time
            time.sleep(0.01)
    
    def checkpoint(self):
        state = super().checkpoint()
        print(f"NumberSource checkpoint: current={self._context.get_state('current')}")
        return state


class SquareOperator(Operator):
    """Operator that squares numbers"""
    
    def process(self, record: Record):
        value = record.value
        number = value['number']
        
        # Square the number
        squared = number * number
        
        return [Record(
            key=record.key,
            value={
                'number': number,
                'squared': squared,
            }
        )]


class FilterEvenOperator(Operator):
    """Operator that filters even numbers"""
    
    def process(self, record: Record):
        number = record.value['number']
        
        # Only keep even numbers
        if number % 2 == 0:
            return [record]
        else:
            return []


class PrintSinkOperator(SinkOperator):
    """Sink that prints results"""
    
    def open(self, context):
        super().open(context)
        self.count = 0
    
    def write(self, record: Record):
        self.count += 1
        print(f"Result #{self.count}: {record.value}")
    
    def close(self):
        print(f"\nProcessed {self.count} records total")


def main():
    """Run the quickstart example"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 80)
    print("Solstice Streaming - Quickstart Example")
    print("=" * 80)
    print()
    
    # Initialize Ray
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    
    # Create state backend (local for this example)
    state_backend = LocalStateBackend('/tmp/solstice/quickstart')
    
    # Create job
    job = Job(
        job_id='quickstart_job',
        state_backend=state_backend,
        checkpoint_interval_secs=10,  # Checkpoint every 10 seconds
        checkpoint_interval_records=20,  # Or every 20 records
    )
    
    print("Creating job pipeline:")
    print("  Source (numbers) -> Square -> Filter (evens) -> Sink (print)")
    print()
    
    # Stage 1: Source - Generate numbers (fixed 1 worker)
    source_stage = Stage(
        stage_id='source',
        operator_class=NumberSource,
        operator_config={'max_num': 50},
        parallelism=1,  # Fixed 1 worker
    )
    
    # Stage 2: Square numbers (fixed 2 workers)
    square_stage = Stage(
        stage_id='square',
        operator_class=SquareOperator,
        operator_config={},
        parallelism=2,  # Fixed 2 workers for parallel processing
    )
    
    # Stage 3: Filter even numbers (fixed 1 worker)
    filter_stage = Stage(
        stage_id='filter',
        operator_class=FilterEvenOperator,
        operator_config={},
        parallelism=1,  # Fixed 1 worker
    )
    
    # Stage 4: Sink - Print results (fixed 1 worker)
    sink_stage = Stage(
        stage_id='sink',
        operator_class=PrintSinkOperator,
        operator_config={},
        parallelism=1,  # Fixed 1 worker
    )
    
    # Build DAG
    job.add_stage(source_stage)
    job.add_stage(square_stage, upstream_stages=['source'])
    job.add_stage(filter_stage, upstream_stages=['square'])
    job.add_stage(sink_stage, upstream_stages=['filter'])
    
    # Initialize and start job
    print("Initializing job...")
    job.initialize()
    
    print("Starting job execution...")
    job.start()
    
    print()
    print("Job is running. Will process 50 numbers.")
    print("Checkpoints will be created every 10 seconds or 20 records.")
    print()
    
    # Monitor for a bit
    time.sleep(15)
    
    # Trigger a manual checkpoint
    print("\nTriggering manual checkpoint...")
    checkpoint_id = job.trigger_checkpoint()
    print(f"Checkpoint created: {checkpoint_id}")
    
    # Let it run a bit more
    time.sleep(5)
    
    # Get status
    status = job.get_status()
    print(f"\nJob status: {status}")
    
    # List checkpoints
    checkpoints = job.list_checkpoints()
    print(f"\nAvailable checkpoints: {checkpoints}")
    
    # Wait for completion
    print("\nWaiting for job to complete...")
    job.wait_for_completion(timeout=60)
    
    # Stop job
    job.stop()
    
    print("\n" + "=" * 80)
    print("Quickstart example completed!")
    print("=" * 80)
    
    # Cleanup
    ray.shutdown()


if __name__ == '__main__':
    main()

