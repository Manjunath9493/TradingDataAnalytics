import os

def install_packages():
    """Installs dependencies at runtime"""
    os.system("pip install -r /dbfs/FileStore/tables/requirements.txt")
