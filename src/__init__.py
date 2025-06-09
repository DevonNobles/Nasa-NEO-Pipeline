import  logging

# Initialize logger
logging.basicConfig(
    level=logging.DEBUG,
    filename='NeoPipeline.log',
    format='%(asctime)s - %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)