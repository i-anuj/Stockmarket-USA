# Auto-Refresh Manager Module for Stock Market Analytics

import streamlit as st
from datetime import datetime, timedelta
from typing import Dict


def initialize_refresh_state():
    """Initialize auto-refresh state in session state if not exists."""
    if 'auto_refresh' not in st.session_state:
        st.session_state['auto_refresh'] = {
            'enabled': True,
            'interval': 60,  # seconds
            'last_refresh_time': datetime.now(),
            'refresh_count': 0,
            'is_refreshing': False
        }


def toggle_refresh() -> bool:
    """
    Toggle auto-refresh on/off.
    
    Returns:
        New state (True if enabled, False if disabled)
    """
    initialize_refresh_state()
    st.session_state['auto_refresh']['enabled'] = not st.session_state['auto_refresh']['enabled']
    return st.session_state['auto_refresh']['enabled']


def is_refresh_enabled() -> bool:
    """
    Check if auto-refresh is enabled.
    
    Returns:
        True if auto-refresh is enabled, False otherwise
    """
    initialize_refresh_state()
    return st.session_state['auto_refresh']['enabled']


def get_refresh_interval() -> int:
    """
    Get the current refresh interval in seconds.
    
    Returns:
        Refresh interval in seconds
    """
    initialize_refresh_state()
    return st.session_state['auto_refresh']['interval']


def set_refresh_interval(seconds: int) -> bool:
    """
    Set the refresh interval.
    
    Args:
        seconds: Refresh interval in seconds (min 10, max 300)
        
    Returns:
        True if set successfully, False if invalid value
    """
    initialize_refresh_state()
    
    # Validate interval
    if seconds < 10 or seconds > 300:
        return False
    
    st.session_state['auto_refresh']['interval'] = seconds
    return True


def should_refresh() -> bool:
    """
    Check if data should be refreshed based on time elapsed.
    
    Returns:
        True if refresh is due, False otherwise
    """
    initialize_refresh_state()
    
    # Don't refresh if disabled
    if not st.session_state['auto_refresh']['enabled']:
        return False
    
    # Don't refresh if currently refreshing
    if st.session_state['auto_refresh']['is_refreshing']:
        return False
    
    # Check if enough time has passed
    time_since_refresh = (datetime.now() - st.session_state['auto_refresh']['last_refresh_time']).total_seconds()
    
    return time_since_refresh >= st.session_state['auto_refresh']['interval']


def get_countdown() -> int:
    """
    Get seconds until next refresh.
    
    Returns:
        Seconds remaining until next refresh (0 if refresh is due)
    """
    initialize_refresh_state()
    
    if not st.session_state['auto_refresh']['enabled']:
        return 0
    
    time_since_refresh = (datetime.now() - st.session_state['auto_refresh']['last_refresh_time']).total_seconds()
    countdown = st.session_state['auto_refresh']['interval'] - time_since_refresh
    
    return max(0, int(countdown))


def mark_refreshed():
    """Mark that a refresh has occurred."""
    initialize_refresh_state()
    st.session_state['auto_refresh']['last_refresh_time'] = datetime.now()
    st.session_state['auto_refresh']['refresh_count'] += 1
    st.session_state['auto_refresh']['is_refreshing'] = False


def start_refreshing():
    """Mark that a refresh is in progress."""
    initialize_refresh_state()
    st.session_state['auto_refresh']['is_refreshing'] = True


def is_refreshing() -> bool:
    """
    Check if a refresh is currently in progress.
    
    Returns:
        True if refreshing, False otherwise
    """
    initialize_refresh_state()
    return st.session_state['auto_refresh']['is_refreshing']


def get_refresh_count() -> int:
    """
    Get the total number of refreshes this session.
    
    Returns:
        Number of refreshes
    """
    initialize_refresh_state()
    return st.session_state['auto_refresh']['refresh_count']


def get_last_refresh_time() -> datetime:
    """
    Get the time of the last refresh.
    
    Returns:
        Datetime of last refresh
    """
    initialize_refresh_state()
    return st.session_state['auto_refresh']['last_refresh_time']


def get_time_since_refresh() -> int:
    """
    Get seconds since last refresh.
    
    Returns:
        Seconds since last refresh
    """
    initialize_refresh_state()
    return int((datetime.now() - st.session_state['auto_refresh']['last_refresh_time']).total_seconds())


def reset_refresh_state():
    """Reset refresh state to defaults."""
    if 'auto_refresh' in st.session_state:
        del st.session_state['auto_refresh']
    initialize_refresh_state()


def get_refresh_state() -> Dict:
    """
    Get complete refresh state.
    
    Returns:
        Dictionary with all refresh state information
    """
    initialize_refresh_state()
    return {
        'enabled': st.session_state['auto_refresh']['enabled'],
        'interval': st.session_state['auto_refresh']['interval'],
        'last_refresh_time': st.session_state['auto_refresh']['last_refresh_time'],
        'refresh_count': st.session_state['auto_refresh']['refresh_count'],
        'is_refreshing': st.session_state['auto_refresh']['is_refreshing'],
        'countdown': get_countdown(),
        'time_since_refresh': get_time_since_refresh()
    }
