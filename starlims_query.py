import sys
import configparser
import pymssql
import argparse
import pandas as pd

# ---- ARGUMENTS -----#
parser = argparse.ArgumentParser(
                    prog='get_sl_data.py',
                    description='This script quries the StarLIMS SQL database and saves it as a comma-separated file.')
parser.add_argument('-q',
                    '--query',
                    help = 'SQL query')
parser.add_argument('-o', 
                    '--output',
                    help = 'Name of output file (e.g., 2023_lims_data.csv)')
parser.add_argument('-c',
                    '--config',
                    help = 'Path to config file (.ini). Used preferentially over --server and --port flags.')
parser.add_argument('-s', 
                    '--server',
                    help = 'Server name. Must be used in combination with server port number.')
parser.add_argument('-p', 
                    '--port',
                    help = 'Server port number. Must be used in combination with server name.')

args = parser.parse_args()

#----- ASSIGN SERVER & POR -----#
if args.port is not None and args.server is not None:
    serv = args.server
    prt = args.port

# via config file
if args.config is not None:
    # load LIMS config file
    config = configparser.ConfigParser()
    config.read(args.config)
    
    serv = config['DEFAULT']['SERVER'].split(',')[0]
    prt = config['DEFAULT']['SERVER'].split(',')[1]

# no server info supplied
if args.port is None and args.server is None and args.config is None:
    sys.exit("Error: Please provide the server name and port number via the --config or --server and --port flags.")

#----- CONNECT TO SERVER -----#
conn = pymssql.connect(server = serv, port = prt)

#----- PERFORM QUERY & SAVE RESULTS -----#
df = pd.read_sql(args.query, conn)
df.to_csv(args.output, sep=',', index = False, encoding = 'utf-8')
