# Watchlist Manager Module for Stock Market Analytics

import streamlit as st
from typing import List
from datetime import datetime


def initialize_watchlist():
    """Initialize watchlist in session state if not exists."""
    if 'watchlist' not in st.session_state:
        st.session_state['watchlist'] = []
    if 'watchlist_last_updated' not in st.session_state:
        st.session_state['watchlist_last_updated'] = datetime.now()
    if 'watchlist_max_size' not in st.session_state:
        st.session_state['watchlist_max_size'] = 20


def add_to_watchlist(symbol: str) -> bool:
    """
    Add a stock symbol to the watchlist.
    
    Args:
        symbol: Stock symbol to add
        
    Returns:
        True if added successfully, False if already exists or limit reached
    """
    initialize_watchlist()
    
    # Check if already in watchlist
    if symbol in st.session_state['watchlist']:
        return False
    
    # Check if watchlist is full
    if len(st.session_state['watchlist']) >= st.session_state['watchlist_max_size']:
        return False
    
    # Add to watchlist
    st.session_state['watchlist'].append(symbol)
    st.session_state['watchlist_last_updated'] = datetime.now()
    
    return True


def remove_from_watchlist(symbol: str) -> bool:
    """
    Remove a stock symbol from the watchlist.
    
    Args:
        symbol: Stock symbol to remove
        
    Returns:
        True if removed successfully, False if not in watchlist
    """
    initialize_watchlist()
    
    if symbol in st.session_state['watchlist']:
        st.session_state['watchlist'].remove(symbol)
        st.session_state['watchlist_last_updated'] = datetime.now()
        return True
    
    return False


def get_watchlist() -> List[str]:
    """
    Get the current watchlist.
    
    Returns:
        Sorted list of stock symbols in watchlist
    """
    initialize_watchlist()
    return sorted(st.session_state['watchlist'])


def is_in_watchlist(symbol: str) -> bool:
    """
    Check if a symbol is in the watchlist.
    
    Args:
        symbol: Stock symbol to check
        
    Returns:
        True if symbol is in watchlist, False otherwise
    """
    initialize_watchlist()
    return symbol in st.session_state['watchlist']


def get_watchlist_count() -> int:
    """
    Get the number of stocks in the watchlist.
    
    Returns:
        Number of stocks in watchlist
    """
    initialize_watchlist()
    return len(st.session_state['watchlist'])


def get_watchlist_max_size() -> int:
    """
    Get the maximum watchlist size.
    
    Returns:
        Maximum number of stocks allowed in watchlist
    """
    initialize_watchlist()
    return st.session_state['watchlist_max_size']


def is_watchlist_full() -> bool:
    """
    Check if watchlist is at maximum capacity.
    
    Returns:
        True if watchlist is full, False otherwise
    """
    initialize_watchlist()
    return len(st.session_state['watchlist']) >= st.session_state['watchlist_max_size']


def clear_watchlist() -> bool:
    """
    Clear all stocks from the watchlist.
    
    Returns:
        True if cleared successfully
    """
    initialize_watchlist()
    st.session_state['watchlist'] = []
    st.session_state['watchlist_last_updated'] = datetime.now()
    return True


def get_last_updated() -> datetime:
    """
    Get the last time the watchlist was updated.
    
    Returns:
        Datetime of last watchlist modification
    """
    initialize_watchlist()
    return st.session_state['watchlist_last_updated']
