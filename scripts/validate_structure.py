#!/usr/bin/env python3
"""
Validation script to verify project structure and imports.
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def check_imports():
    """Check if all modules can be imported."""
    print("🔍 Checking imports...")
    
    try:
        from src import config
        print("  ✅ config")
    except ImportError as e:
        print(f"  ❌ config: {e}")
        return False
    
    try:
        from src.core import technical_indicators
        print("  ✅ technical_indicators")
    except ImportError as e:
        print(f"  ❌ technical_indicators: {e}")
        return False
    
    try:
        from src.core import data_processor
        print("  ✅ data_processor")
    except ImportError as e:
        print(f"  ❌ data_processor: {e}")
        return False
    
    try:
        from src.services import api_service
        print("  ✅ api_service")
    except ImportError as e:
        print(f"  ❌ api_service: {e}")
        return False
    
    try:
        from src.managers import watchlist_manager
        print("  ✅ watchlist_manager")
    except ImportError as e:
        print(f"  ❌ watchlist_manager: {e}")
        return False
    
    try:
        from src.managers import refresh_manager
        print("  ✅ refresh_manager")
    except ImportError as e:
        print(f"  ❌ refresh_manager: {e}")
        return False
    
    try:
        from src.ui import charts
        print("  ✅ charts")
    except ImportError as e:
        print(f"  ❌ charts: {e}")
        return False
    
    try:
        from src.ui import components
        print("  ✅ components")
    except ImportError as e:
        print(f"  ❌ components: {e}")
        return False
    
    return True

def check_structure():
    """Check if required directories and files exist."""
    print("\n🔍 Checking project structure...")
    
    required_dirs = [
        "src",
        "src/core",
        "src/services",
        "src/managers",
        "src/ui",
        "src/utils",
        "tests",
        "docs",
        "docker",
        "scripts",
        "config",
        ".github/workflows"
    ]
    
    required_files = [
        "src/__init__.py",
        "src/app.py",
        "src/config.py",
        "src/core/__init__.py",
        "src/core/technical_indicators.py",
        "src/core/data_processor.py",
        "src/services/__init__.py",
        "src/services/api_service.py",
        "src/managers/__init__.py",
        "src/managers/watchlist_manager.py",
        "src/managers/refresh_manager.py",
        "src/ui/__init__.py",
        "src/ui/charts.py",
        "src/ui/components.py",
        "tests/__init__.py",
        "docker/Dockerfile",
        "docker/docker-compose.yml",
        "README.md",
        "requirements.txt",
        "setup.py",
        "pyproject.toml",
        "LICENSE",
        "CHANGELOG.md"
    ]
    
    all_exist = True
    
    for dir_path in required_dirs:
        full_path = project_root / dir_path
        if full_path.exists():
            print(f"  ✅ {dir_path}/")
        else:
            print(f"  ❌ {dir_path}/ (missing)")
            all_exist = False
    
    for file_path in required_files:
        full_path = project_root / file_path
        if full_path.exists():
            print(f"  ✅ {file_path}")
        else:
            print(f"  ❌ {file_path} (missing)")
            all_exist = False
    
    return all_exist

def check_documentation():
    """Check if documentation files exist."""
    print("\n🔍 Checking documentation...")
    
    doc_files = [
        "docs/README.md",
        "docs/ARCHITECTURE.md",
        "docs/DEVELOPMENT.md",
        "docs/DEPLOYMENT_GUIDE.md"
    ]
    
    all_exist = True
    
    for doc_file in doc_files:
        full_path = project_root / doc_file
        if full_path.exists():
            print(f"  ✅ {doc_file}")
        else:
            print(f"  ❌ {doc_file} (missing)")
            all_exist = False
    
    return all_exist

def check_tests():
    """Check if test files exist."""
    print("\n🔍 Checking tests...")
    
    test_files = [
        "tests/test_technical_indicators.py",
        "tests/test_api_service.py",
        "tests/test_managers.py"
    ]
    
    all_exist = True
    
    for test_file in test_files:
        full_path = project_root / test_file
        if full_path.exists():
            print(f"  ✅ {test_file}")
        else:
            print(f"  ❌ {test_file} (missing)")
            all_exist = False
    
    return all_exist

def main():
    """Run all validation checks."""
    print("=" * 60)
    print("Stock Market Dashboard - Structure Validation")
    print("=" * 60)
    
    results = {
        "Structure": check_structure(),
        "Imports": check_imports(),
        "Documentation": check_documentation(),
        "Tests": check_tests()
    }
    
    print("\n" + "=" * 60)
    print("Validation Results")
    print("=" * 60)
    
    for check, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{check}: {status}")
    
    all_passed = all(results.values())
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 All validation checks passed!")
        print("✅ Project structure is production-ready")
        return 0
    else:
        print("⚠️  Some validation checks failed")
        print("❌ Please fix the issues above")
        return 1

if __name__ == "__main__":
    sys.exit(main())
