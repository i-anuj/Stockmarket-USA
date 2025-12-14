import pytest
from unittest.mock import patch
from src.managers import watchlist_manager, refresh_manager


class TestWatchlistManager:
    """Test cases for watchlist manager."""
    
    @patch('src.managers.watchlist_manager.st')
    def test_add_to_watchlist(self, mock_st):
        """Test adding stock to watchlist."""
        # Mock session state
        mock_st.session_state = {
            'watchlist': [],
            'watchlist_max_size': 20
        }
        
        result = watchlist_manager.add_to_watchlist("IBM")
        assert result is True
        assert "IBM" in mock_st.session_state['watchlist']
    
    @patch('src.managers.watchlist_manager.st')
    def test_add_duplicate_to_watchlist(self, mock_st):
        """Test adding duplicate stock to watchlist."""
        # Mock session state with existing stock
        mock_st.session_state = {
            'watchlist': ['IBM'],
            'watchlist_max_size': 20
        }
        
        result = watchlist_manager.add_to_watchlist("IBM")
        assert result is False
    
    @patch('src.managers.watchlist_manager.st')
    def test_remove_from_watchlist(self, mock_st):
        """Test removing stock from watchlist."""
        # Mock session state with existing stock
        mock_st.session_state = {
            'watchlist': ['IBM', 'AAPL'],
            'watchlist_max_size': 20
        }
        
        result = watchlist_manager.remove_from_watchlist("IBM")
        assert result is True
        assert "IBM" not in mock_st.session_state['watchlist']
        assert "AAPL" in mock_st.session_state['watchlist']


class TestRefreshManager:
    """Test cases for refresh manager."""
    
    @patch('src.managers.refresh_manager.st')
    def test_toggle_refresh(self, mock_st):
        """Test toggling refresh state."""
        # Mock session state
        mock_st.session_state = {
            'auto_refresh': {
                'enabled': True,
                'interval': 60,
                'last_refresh_time': None,
                'refresh_count': 0,
                'is_refreshing': False
            }
        }
        
        result = refresh_manager.toggle_refresh()
        assert result is False  # Should be toggled to False
        assert mock_st.session_state['auto_refresh']['enabled'] is False
