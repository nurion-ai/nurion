"""Sink operators for writing data"""

import logging
from typing import Any, Dict, Optional
from pathlib import Path

from solstice.core.operator import SinkOperator
from solstice.core.models import Record


class Sink(SinkOperator):
    """Base sink operator"""
    pass


class PrintSink(Sink):
    """Sink that prints records to stdout"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.count = 0
    
    def write(self, record: Record) -> None:
        """Print record"""
        self.count += 1
        print(f"[{self.count}] Key: {record.key}, Value: {record.value}")
    
    def close(self) -> None:
        """Print summary"""
        self.logger.info(f"Printed {self.count} records")


class FileSink(Sink):
    """Sink that writes records to a file"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        
        self.output_path = config.get('output_path')
        self.format = config.get('format', 'json')  # json, parquet, csv
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.buffer = []
        self.buffer_size = config.get('buffer_size', 1000)
        self.file_handle = None
    
    def open(self, context) -> None:
        """Open output file"""
        super().open(context)
        
        # Create output directory if needed
        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        
        if self.format == 'json':
            self.file_handle = open(self.output_path, 'w')
        
        self.logger.info(f"Opened output file: {self.output_path}")
    
    def write(self, record: Record) -> None:
        """Write record to file"""
        self.buffer.append(record)
        
        if len(self.buffer) >= self.buffer_size:
            self._flush()
    
    def _flush(self) -> None:
        """Flush buffer to file"""
        if not self.buffer:
            return
        
        if self.format == 'json':
            self._flush_json()
        elif self.format == 'parquet':
            self._flush_parquet()
        elif self.format == 'csv':
            self._flush_csv()
        
        self.buffer.clear()
    
    def _flush_json(self) -> None:
        """Flush as JSON lines"""
        import json
        
        for record in self.buffer:
            json_line = json.dumps({
                'key': record.key,
                'value': record.value,
                'timestamp': record.timestamp,
                'metadata': record.metadata,
            })
            self.file_handle.write(json_line + '\n')
    
    def _flush_parquet(self) -> None:
        """Flush as Parquet"""
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        # Convert records to table
        data = {
            'key': [r.key for r in self.buffer],
            'value': [r.value for r in self.buffer],
            'timestamp': [r.timestamp for r in self.buffer],
        }
        
        table = pa.Table.from_pydict(data)
        
        # Append to parquet file
        if Path(self.output_path).exists():
            pq.write_table(table, self.output_path, append=True)
        else:
            pq.write_table(table, self.output_path)
    
    def _flush_csv(self) -> None:
        """Flush as CSV"""
        import csv
        
        # Assume value is a dict
        if not self.buffer:
            return
        
        # Get fieldnames from first record
        fieldnames = ['key'] + list(self.buffer[0].value.keys())
        
        file_exists = Path(self.output_path).exists()
        
        with open(self.output_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            for record in self.buffer:
                row = {'key': record.key}
                row.update(record.value)
                writer.writerow(row)
    
    def close(self) -> None:
        """Close file and flush remaining data"""
        self._flush()
        
        if self.file_handle:
            self.file_handle.close()
        
        self.logger.info(f"Closed output file: {self.output_path}")


class LanceSink(Sink):
    """Sink that writes to a Lance table"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        
        self.table_path = config.get('table_path')
        self.mode = config.get('mode', 'append')  # append, overwrite
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.buffer = []
        self.buffer_size = config.get('buffer_size', 1000)
        self.table = None
    
    def open(self, context) -> None:
        """Initialize Lance table"""
        super().open(context)
        
        try:
            import lance
            self.lance = lance
        except ImportError:
            raise ImportError("lance library required for LanceSink")
        
        # Create output directory
        Path(self.table_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Initialized Lance sink: {self.table_path}")
    
    def write(self, record: Record) -> None:
        """Write record to buffer"""
        self.buffer.append(record.value)
        
        if len(self.buffer) >= self.buffer_size:
            self._flush()
    
    def _flush(self) -> None:
        """Flush buffer to Lance table"""
        if not self.buffer:
            return
        
        import pyarrow as pa
        
        # Convert to PyArrow table
        table = pa.Table.from_pylist(self.buffer)
        
        # Write to Lance
        if self.table is None:
            # Create new table
            self.table = self.lance.write_dataset(
                table,
                self.table_path,
                mode=self.mode
            )
        else:
            # Append to existing table
            self.lance.write_dataset(
                table,
                self.table_path,
                mode='append'
            )
        
        self.logger.info(f"Flushed {len(self.buffer)} records to Lance table")
        self.buffer.clear()
    
    def close(self) -> None:
        """Flush remaining data"""
        self._flush()
        self.logger.info(f"Closed Lance sink: {self.table_path}")

