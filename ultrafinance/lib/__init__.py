''' ultraFinance lib '''
import logging

logging.basicConfig(
        level=logging.DEBUG, format='%(asctime)s%(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        datefmt='%a, %d %b %Y %H:%M:%S')

# logging.basicConfig(
#         level=logging.INFO, format='%(asctime)s%(filename)s[line:%(lineno)d] %(levelname)s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')

LOG = logging.getLogger(__name__)

'''
mainSrc = os.path.join(os.path.dirname( (os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(mainSrc)

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d %(levelname)-5.5s [%(name)s] [%(threadName)s] %(message)s',
                    filename=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ultraFinance.log'),
                    filemode='w')
'''
