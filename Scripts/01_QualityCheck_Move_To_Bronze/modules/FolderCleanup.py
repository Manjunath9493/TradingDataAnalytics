def Folder_Cleanup(path):
    files = dbutils.fs.ls(path)
    for f in files:
        dbutils.fs.rm(f.path)