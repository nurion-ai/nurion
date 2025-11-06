"""Source operators for reading data"""

import logging
from typing import Any, Dict, Iterable, Optional
from pathlib import Path

from solstice.core.operator import SourceOperator
from solstice.core.models import Record


class IcebergSource(SourceOperator):
    """Source operator for reading from Iceberg tables"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        
        self.catalog_uri = config.get('catalog_uri')
        self.table_name = config.get('table_name')
        self.batch_size = config.get('batch_size', 1000)
        self.filter_expr = config.get('filter')
        self.snapshot_id = config.get('snapshot_id')
        
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Iceberg table handle
        self.catalog = None
        self.table = None
        self.scan = None
        self.current_offset = 0
    
    def open(self, context) -> None:
        """Initialize the Iceberg table connection"""
        super().open(context)
        
        try:
            from pyiceberg.catalog import load_catalog
            
            if not self.catalog_uri:
                raise ValueError("catalog_uri is required for IcebergSource")
            if not self.table_name:
                raise ValueError("table_name is required for IcebergSource")
            
            # Load catalog
            self.catalog = load_catalog(
                name="default",
                **{"uri": self.catalog_uri}
            )
            
            # Load table
            self.table = self.catalog.load_table(self.table_name)
            
            # Create scan
            scan = self.table.scan()
            
            if self.filter_expr:
                scan = scan.filter(self.filter_expr)
            
            if self.snapshot_id:
                scan = scan.use_snapshot(self.snapshot_id)
            
            self.scan = scan
            
            # Get offset from state if recovering
            if self._context:
                self.current_offset = self._context.get_state('offset', 0)
            
            self.logger.info(
                f"Opened Iceberg table {self.table_name}, "
                f"starting from offset {self.current_offset}"
            )
            
        except ImportError:
            raise ImportError(
                "pyiceberg library is required for IcebergSource. "
                "Install it with: pip install pyiceberg"
            )
    
    def read(self) -> Iterable[Record]:
        """Read records from Iceberg table"""
        if not self.scan:
            raise RuntimeError("Source not opened. Call open() first.")
        
        # Read in batches
        for task in self.scan.plan_files():
            # Read file task
            arrow_batch_reader = task.to_arrow()
            
            for batch in arrow_batch_reader:
                # Skip batches before current offset
                if self.current_offset > 0:
                    batch_size = len(batch)
                    if self.current_offset >= batch_size:
                        self.current_offset -= batch_size
                        continue
                    else:
                        # Partial skip within batch
                        batch = batch.slice(self.current_offset)
                        self.current_offset = 0
                
                # Convert batch to records
                batch_dict = batch.to_pydict()
                num_rows = len(batch)
                
                for i in range(num_rows):
                    # Create record from row
                    row = {col: batch_dict[col][i] for col in batch_dict.keys()}
                    
                    # Use first column as key if available
                    key = None
                    if batch_dict:
                        first_col = list(batch_dict.keys())[0]
                        key = str(row[first_col])
                    
                    record = Record(
                        key=key,
                        value=row,
                        metadata={'source': 'iceberg', 'table': self.table_name}
                    )
                    
                    yield record
                    
                    # Update offset
                    if self._context:
                        self._context.set_state('offset', self._context.get_state('offset', 0) + 1)
    
    def checkpoint(self) -> Dict[str, Any]:
        """Checkpoint the current offset"""
        state = super().checkpoint()
        if self._context:
            state['offset'] = self._context.get_state('offset', 0)
        return state
    
    def close(self) -> None:
        """Close the Iceberg table"""
        self.scan = None
        self.table = None
        self.catalog = None
        self.logger.info("Closed Iceberg table source")


class LanceTableSource(SourceOperator):
    """Source operator for reading from Lance tables"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        
        self.table_path = config.get('table_path')
        self.batch_size = config.get('batch_size', 1000)
        self.columns = config.get('columns')  # None = all columns
        self.filter_expr = config.get('filter')  # Optional filter expression
        
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Lance table handle
        self.table = None
        self.scanner = None
        self.current_offset = 0
    
    def open(self, context) -> None:
        """Initialize the Lance table connection"""
        super().open(context)
        
        try:
            import lance
            
            if not Path(self.table_path).exists():
                raise FileNotFoundError(f"Lance table not found: {self.table_path}")
            
            self.table = lance.dataset(self.table_path)
            
            # Create scanner
            scanner_kwargs = {}
            if self.columns:
                scanner_kwargs['columns'] = self.columns
            if self.filter_expr:
                scanner_kwargs['filter'] = self.filter_expr
            
            self.scanner = self.table.scanner(**scanner_kwargs)
            
            # Get offset from state if recovering
            if self._context:
                self.current_offset = self._context.get_state('offset', 0)
            
            self.logger.info(
                f"Opened Lance table {self.table_path}, "
                f"starting from offset {self.current_offset}"
            )
            
        except ImportError:
            raise ImportError(
                "lance library is required for LanceTableSource. "
                "Install it with: pip install pylance"
            )
    
    def read(self) -> Iterable[Record]:
        """Read records from Lance table"""
        if not self.scanner:
            raise RuntimeError("Source not opened. Call open() first.")
        
        # Read in batches
        for batch in self.scanner.to_batches(batch_size=self.batch_size):
            # Skip batches before current offset
            if self.current_offset > 0:
                batch_size = len(batch)
                if self.current_offset >= batch_size:
                    self.current_offset -= batch_size
                    continue
                else:
                    # Partial skip within batch
                    batch = batch.slice(self.current_offset)
                    self.current_offset = 0
            
            # Convert batch to records
            batch_dict = batch.to_pydict()
            num_rows = len(batch)
            
            for i in range(num_rows):
                # Create record from row
                row = {col: batch_dict[col][i] for col in batch_dict.keys()}
                
                # Use first column as key if available
                key = None
                if batch_dict:
                    first_col = list(batch_dict.keys())[0]
                    key = str(row[first_col])
                
                record = Record(
                    key=key,
                    value=row,
                    metadata={'source': 'lance', 'table': self.table_path}
                )
                
                yield record
                
                # Update offset
                if self._context:
                    self._context.set_state('offset', self._context.get_state('offset', 0) + 1)
    
    def checkpoint(self) -> Dict[str, Any]:
        """Checkpoint the current offset"""
        state = super().checkpoint()
        if self._context:
            state['offset'] = self._context.get_state('offset', 0)
        return state
    
    def close(self) -> None:
        """Close the Lance table"""
        self.scanner = None
        self.table = None
        self.logger.info("Closed Lance table source")


class FileSource(SourceOperator):
    """Source operator for reading from files"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        
        self.file_paths = config.get('file_paths', [])
        self.file_format = config.get('format', 'json')  # json, parquet, csv
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.current_file_idx = 0
        self.current_row_idx = 0
    
    def open(self, context) -> None:
        """Initialize file reading"""
        super().open(context)
        
        if self._context:
            self.current_file_idx = self._context.get_state('file_idx', 0)
            self.current_row_idx = self._context.get_state('row_idx', 0)
        
        self.logger.info(
            f"Opened file source with {len(self.file_paths)} files, "
            f"starting from file {self.current_file_idx}, row {self.current_row_idx}"
        )
    
    def read(self) -> Iterable[Record]:
        """Read records from files"""
        for file_idx in range(self.current_file_idx, len(self.file_paths)):
            file_path = self.file_paths[file_idx]
            
            if self.file_format == 'json':
                records = self._read_json(file_path)
            elif self.file_format == 'parquet':
                records = self._read_parquet(file_path)
            elif self.file_format == 'csv':
                records = self._read_csv(file_path)
            else:
                raise ValueError(f"Unsupported format: {self.file_format}")
            
            for row_idx, record in enumerate(records):
                # Skip rows before current offset in current file
                if file_idx == self.current_file_idx and row_idx < self.current_row_idx:
                    continue
                
                yield record
                
                # Update state
                if self._context:
                    self._context.set_state('file_idx', file_idx)
                    self._context.set_state('row_idx', row_idx + 1)
            
            # Reset row index for next file
            self.current_row_idx = 0
    
    def _read_json(self, file_path: str) -> Iterable[Record]:
        """Read JSON lines file"""
        import json
        
        with open(file_path, 'r') as f:
            for line in f:
                data = json.loads(line.strip())
                yield Record(value=data, metadata={'source': 'json', 'file': file_path})
    
    def _read_parquet(self, file_path: str) -> Iterable[Record]:
        """Read Parquet file"""
        import pyarrow.parquet as pq
        
        table = pq.read_table(file_path)
        batch_dict = table.to_pydict()
        num_rows = len(table)
        
        for i in range(num_rows):
            row = {col: batch_dict[col][i] for col in batch_dict.keys()}
            yield Record(value=row, metadata={'source': 'parquet', 'file': file_path})
    
    def _read_csv(self, file_path: str) -> Iterable[Record]:
        """Read CSV file"""
        import csv
        
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield Record(value=row, metadata={'source': 'csv', 'file': file_path})
    
    def checkpoint(self) -> Dict[str, Any]:
        """Checkpoint current position"""
        state = super().checkpoint()
        if self._context:
            state['file_idx'] = self._context.get_state('file_idx', 0)
            state['row_idx'] = self._context.get_state('row_idx', 0)
        return state

