#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 15:25:19 2017

@author: Jamie
"""

import os
import pickle

# read database
db = pickle.load(open('db_thin.p', 'rb'))

for pid,j in db.items():
    print(j['_rawid'])

#  idvv = '%sv%d' % (j['_rawid'], j['_version']) #combine paper ID and verison
#  folder = idvv.split("/")[-1][0:4] # extract year and month for folder name
#  idvv = idvv.replace("/","") #remove forward slash from legacy arxiv ids
#  pdf_path = os.path.join('data', 'pdf', idvv) + '.pdf' # where I store pdfs
#
#  if not os.path.isfile(pdf_path):
#      filename = idvv.split('v')[0] # the filenames in the arxiv tarballs
#      pdf_path_alt = os.path.join('data', folder, filename) + '.pdf' # dump path
#
#    if os.path.isfile(pdf_path_alt):
#        os.rename(pdf_path_alt,pdf_path)
#        print("Moved file from %s to %s" % (pdf_path_alt,pdf_path))
