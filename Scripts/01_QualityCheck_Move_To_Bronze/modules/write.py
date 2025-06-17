def write(path,writeformat,writemode):
    log_df.write.mode(writemode).format(writeformat).save(path)