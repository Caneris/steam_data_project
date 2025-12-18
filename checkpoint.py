"""
Reusable checkpoint management for scrapers.

Provides a generic CheckpointManager that works with any picklable data,
plus a base dataclass for type-safe checkpoint definitions.

Usage:
    from checkpoint import CheckpointManager, BaseCheckpoint
    
    @dataclass
    class MyCheckpoint(BaseCheckpoint):
        completed: set[int] = field(default_factory=set)
        failed: dict[int, str] = field(default_factory=dict)
    
    manager = CheckpointManager("checkpoints")
    checkpoint = manager.load("my_scraper", MyCheckpoint) or MyCheckpoint()
    
    # ... do work ...
    
    manager.save("my_scraper", checkpoint)
"""

import pickle
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import TypeVar, Type

T = TypeVar('T')


@dataclass
class BaseCheckpoint:
    """
    Base class for scraper checkpoints.
    
    Subclass this and add your own fields. The base class tracks
    timing information automatically.
    """
    started_at: datetime | None = None
    last_saved_at: datetime | None = None
    
    def mark_started(self) -> None:
        """Call when scraping begins."""
        if self.started_at is None:
            self.started_at = datetime.now()


class CheckpointManager:
    """
    Generic checkpoint manager that handles save/load for any picklable data.
    
    Works with both simple types (dict, set, list) and dataclasses.
    When saving dataclasses with a `last_saved_at` attribute, it's 
    automatically updated.
    """
    
    def __init__(self, directory: str | Path = "checkpoints"):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
    
    def _get_path(self, name: str) -> Path:
        return self.directory / f"{name}.pkl"
    
    def save(self, name: str, data) -> None:
        """
        Save any picklable object to a checkpoint file.
        
        Args:
            name: Checkpoint name (without extension)
            data: Any picklable object
        """
        path = self._get_path(name)
        
        # Auto-update timestamp if available
        if hasattr(data, 'last_saved_at'):
            data.last_saved_at = datetime.now()
        
        with open(path, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    def load(self, name: str, expected_type: Type[T] | None = None) -> T | None:
        """
        Load a checkpoint file.
        
        Args:
            name: Checkpoint name (without extension)
            expected_type: Optional type to validate against
            
        Returns:
            The loaded data, or None if file doesn't exist
            
        Raises:
            TypeError: If expected_type is provided and data doesn't match
        """
        path = self._get_path(name)
        
        if not path.exists():
            return None
        
        with open(path, 'rb') as f:
            data = pickle.load(f)
        
        if expected_type is not None and not isinstance(data, expected_type):
            raise TypeError(
                f"Checkpoint '{name}' contains {type(data).__name__}, "
                f"expected {expected_type.__name__}"
            )
        
        return data
    
    def exists(self, name: str) -> bool:
        """Check if a checkpoint file exists."""
        return self._get_path(name).exists()
    
    def delete(self, name: str) -> bool:
        """
        Delete a checkpoint file.
        
        Returns:
            True if file was deleted, False if it didn't exist
        """
        path = self._get_path(name)
        if path.exists():
            path.unlink()
            return True
        return False
    
    def list_checkpoints(self) -> list[str]:
        """List all checkpoint names in the directory."""
        return [p.stem for p in self.directory.glob("*.pkl")]