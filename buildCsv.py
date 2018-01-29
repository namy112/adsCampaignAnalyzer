import csv, itertools
def build_csv(lines, header=None, file=None):
    print(lines['account_id'])
    if header:
        lines = itertools.chain([header], lines)
    #writer = csv.writer(file, delimiter=',')
   # writer.writerows(lines)
    
    file.seek(0)
    return file