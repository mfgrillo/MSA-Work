#!/usr/bin/env python
# coding: utf-8



import io
import zstandard as zstd
import json
import pandas as pd
import glob
import os




joined_files = os.path.join("C:/Users/marco/OneDrive/Documentos/Gtech/Classes/CSE 8803 DSN/Project/PushshiftDumps-master/scripts/Posts", "*.csv")
joined_list = glob.glob(joined_files)
#print(joined_list)
df = pd.concat(map(pd.read_csv, joined_list), ignore_index=True)
df.head()
df.to_csv('submissions.csv', index = False)

print(len(df))
df_new = df.drop_duplicates()
df_new.to_csv('submissions_no_dups.csv', index = False)
print(len(df_new))



filenames = [
            'RC_2014-01.zst', 'RC_2014-02.zst', 'RC_2014-03.zst', 'RC_2014-04.zst',\
            'RC_2014-05.zst', 'RC_2014-06.zst', 'RC_2014-07.zst', 'RC_2014-08.zst',\
            'RC_2014-09.zst', 'RC_2014-10.zst', 'RC_2014-11.zst', 'RC_2014-12.zst',\
            'RC_2015-01.zst', 'RC_2015-02.zst', 'RC_2015-03.zst', 'RC_2015-04.zst',\
            'RC_2015-05.zst', 'RC_2015-06.zst', 'RC_2015-07.zst', 'RC_2015-08.zst',\
            'RC_2015-09.zst', 'RC_2015-10.zst', 'RC_2015-11.zst', 'RC_2015-12.zst',\
            'RC_2016-01.zst', 'RC_2016-02.zst', 'RC_2016-03.zst', 'RC_2016-04.zst',\
            'RC_2016-05.zst', 'RC_2016-06.zst', 'RC_2016-07.zst', 'RC_2016-08.zst',\
            'RC_2016-09.zst', 'RC_2016-10.zst', 'RC_2016-11.zst', 'RC_2016-12.zst',\
            'RC_2017-01.zst', 'RC_2017-02.zst', 'RC_2017-03.zst', 'RC_2017-04.zst'
            ]

def length_reader(filename):
    with open(filename, 'rb') as fh:
        dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
        stream_reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
        count = 1
        for line in text_stream:
             count += 1
        return_dict = {'filename': filename, 'length': count}
        return return_dict

filenames_dict_comments = []
    
for i in filenames:
    filenames_dict_comments.append(length_reader(i))
    
print(filenames_dict_comments)


filenames_dict_subs = [
    {'filename': 'RS_2017-01.zst', 'length': 9218514},
    {'filename': 'RS_2017-02.zst', 'length': 8588121},
    {'filename': 'RS_2017-03.zst', 'length': 9616341},
    {'filename': 'RS_2017-04.zst', 'length': 9211052}
]


def processor(filename, length):
    with open(filename, 'rb') as fh:
        dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
        stream_reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
        count = 1
        cols = ['subreddit_id', 'selftext', 'author', 'over_18', 'ups', 'created_utc', 'score', 'downs',            'title','num_comments', 'subreddit', 'quarantine']
        df = pd.DataFrame({'subreddit_id': pd.Series(dtype='str'),
                       'selftext': pd.Series(dtype='str'),
                       'author': pd.Series(dtype='str'),
                       'over_18': pd.Series(dtype='bool'),
                       'ups': pd.Series(dtype='int'),
                       'created_utc': pd.Series(dtype='str'),
                       'score': pd.Series(dtype='int'),
                       'downs': pd.Series(dtype='int'),
                       'title': pd.Series(dtype='str'),
                       'num_comments': pd.Series(dtype='int'),
                       'subreddit': pd.Series(dtype='str'),
                       'quarantine': pd.Series(dtype='bool')})
        divider = round(length/374999)
        part_counts = 1
        for line in text_stream:
            if count % divider != 0:
                count += 1
                continue
            obj = json.loads(line)
            if 'promoted' in obj:
                continue
            to_add = [obj['subreddit_id'], obj['selftext'], obj['author'], obj['over_18'], 0, obj['created_utc'],obj['score'], 0, obj['title'], obj['num_comments'],                      obj['subreddit'], obj['quarantine']]
            df.loc[len(df)] = to_add
            count += 1
            if len(df) == 50000:
                print('Saving ' + filename + str(part_counts))
                df.to_csv(filename[:len(filename)-4] + str(part_counts) + '.csv', index = False)
                df = df[0:0]
                part_counts += 1
        print('Saving ' + filename)
        df.to_csv(filename[:len(filename)-4] + str(part_counts) + '.csv', index = False)
        
for i in filenames_dict_subs:
    processor(i['filename'], i['length'])


filenames_dict_coms = [
    {"filename": "RC_2017-02.zst", "length": 38703363},
    {"filename": "RC_2017-03.zst", "length": 42459957},
    {"filename": "RC_2017-04.zst", "length": 42440736}
]


def processor_coms(filename, length):
    with open(filename, 'rb') as fh:
        dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
        stream_reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
        count = 1
        cols = ['author_flair_text', 'controversiality', 'parent_id', 'body', 'subreddit', 'id',                'score', 'subreddit_id']
        df = pd.DataFrame({'author_flair_text': pd.Series(dtype='str'),
                       'controversiality': pd.Series(dtype='int'),
                       'parent_id': pd.Series(dtype='str'),
                       'body': pd.Series(dtype='str'),
                       'subreddit': pd.Series(dtype='str'),
                       'id': pd.Series(dtype='str'),
                       'score': pd.Series(dtype='int'),
                       'subreddit_id': pd.Series(dtype='str')})
        divider = round(length/225000)
        part_counts = 1
        for line in text_stream:
            if count % divider != 0:
                count += 1
                continue
            obj = json.loads(line)
            to_add = [obj['author_flair_text'], obj['controversiality'], obj['parent_id'], obj['body'],                      obj['subreddit'], obj['id'], obj['score'], obj['subreddit_id']]
            df.loc[len(df)] = to_add
            count += 1
            if len(df) == 50000:
                print('Saving ' + filename + str(part_counts))
                df.to_csv(filename[:len(filename)-4] + str(part_counts) + '.csv', index = False)
                df = df[0:0]
                part_counts += 1
        print('Saving ' + filename + str(part_counts))
        df.to_csv(filename[:len(filename)-4] + str(part_counts) + '.csv', index = False)
        
for i in filenames_dict_coms:
    processor_coms(i['filename'], i['length'])

