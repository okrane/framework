"""
    Read the file config.json in the same directory as this script

"""
from os import listdir
from os.path import isfile, join, realpath, dirname, exists
from shutil import copyfile
import logging
import json
from pprint import pprint
from sys import exit
import argparse
import types


def get_conf_file():
    config_file = join(dirname(realpath(__file__)), 'config.json') 
    json_file   = open(config_file)
    config      = json.loads(json_file.read())
    json_file.close()
    return config


CONF = get_conf_file()

def list_file(mypath):
    return [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]

def list_dir(mypath):
    return [ f for f in listdir(mypath) if not isfile(join(mypath,f)) ]

def copy_source_code(root_path, env_from, user_from, env_to, user_to):
    root_path_from  = join(root_path, env_from)
    root_path_to    = join(root_path, env_to)
    folder_to_copy  = join(root_path_from, user_from, user_from + 'USR')
    file_to_copy    = list_file(folder_to_copy)
    d               = {}
    for f in file_to_copy:
        destination_path            = join(root_path_to, user_to, user_to + 'USR')
        d[join(folder_to_copy,f)]   = join(destination_path, f)
        
    for k in sorted(d.keys()):
        logging.debug( k + '\t\t=>\t' + d[k] )
        copyfile(k, d[k])
        
    return d
def copy_specific_views(root_path, env_from, user_from, env_to, user_to):
    root_path_from  = join(root_path, env_from)
    root_path_to    = join(root_path, env_to)
    folder_to_copy  = join(root_path_from, user_from)
    file_to_copy    = list_file(folder_to_copy)
    d               = {}
    for f in file_to_copy:
        file_name = f
        if f[0:len(user_from)] == user_from:
            file_name       = user_to + f[len(user_from):]
            
        destination_path            = join(root_path_to, user_to, user_to + 'USR')
        d[join(folder_to_copy,f)]   = join(destination_path, file_name)
        
        
    for k in sorted(d.keys()):
        logging.debug( k + '\t\t=>\t' + d[k] )
        copyfile(k, d[k])
    return d

def copy_repo(root_path, env_from, user_from, env_to, user_to):
    copy_specific_views(root_path,  env_from, user_from, env_to, user_to)
    copy_source_code(root_path,     env_from, user_from, env_to, user_to)
    

def is_valid_user(user):
    if user in CONF['Users']:
        return True
    logging.error('This user [%s] does not exist' % user)
    return False
def is_valid_env(env):
    if env in CONF['Environments']:
        return True
    logging.error('This environment [%s] does not exist' % env)
    return False
def verification_to_prod(user_from, user_to):
    if user_from in CONF['ExcludedForSetProd'] :
        return False
    return True and is_valid_user(user_from) and is_valid_user(user_to)
    
def set_preprod(root_path, env_from, user_from, env_to, user_to):
    copy_repo(root_path, env_from, user_from, env_to, user_to)
    
def set_prod(root_path, env_from, user_from):
    for user_to in CONF['Users']:
        if verification_to_prod(user_from, user_to):
            for env_to in CONF['Environments']:
                copy_repo(root_path, env_from, user_from, env_to, user_to)
def verify(root_path, env_from, user_from, env_to, user_to):  
    if not exists(root_path):
        logging.error('This folder does not exist: ' + root_path )
        return False
    if not is_valid_env(env_to) or not is_valid_env(env_from) or not is_valid_user(user_to) or not is_valid_user(user_from):
        return False
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Set PreProd Or Prod by copying')
    parser.add_argument('-u', '--user_from', help='User From', type=types.StringType)
    parser.add_argument('-d', '--user_to', help='User To', default="ON1")
    parser.add_argument('-e', '--env_from', help='Environment From', default="Dev.30")
    parser.add_argument('-t', '--env_to', help='Environment To', default="Prod.50")
    parser.add_argument('-p', '--to_prod', help='Set PROD from this User*Env', default=False, type=types.BooleanType)
    
    args = parser.parse_args()
    
    root_path = dirname(realpath(__file__))
    
    if not verify(root_path = root_path, 
                  env_from  = args.env_from, 
                  user_from = args.user_from, 
                  env_to    = args.env_to, 
                  user_to   = args.user_to):
        exit(1)
    
    if args.to_prod:
        set_prod(root_path, args.env_from, args.user_from)
    else:
        set_preprod(root_path = root_path, 
                    env_from  = args.env_from, 
                    user_from = args.user_from, 
                    env_to    = args.env_to, 
                    user_to   = args.user_to)
    logging.warning('Please now commit the whole directory to tqke into account the modifications')
#copy_repo(root_path ='C:\\temp\\Git Frontend', env_from ='Dev 30', user_from = 'ON1', env_to = 'Prod.73', user_to = 'ON1')