import os
import logging
import socket
import numpy as np
import time
_, n = os.path.split(os.path.abspath(__file__))

logging.basicConfig()
logger = logging.getLogger(n)



def isiter(a):
    try:
        (x for x in a)
        return True
    except TypeError:
        return False


def getNetworkIp():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.connect(('<broadcast>', 0))
    ip = s.getsockname()[0]
    s.close()
    return ip

def get_local_ip():
    try:
        '''Get the IP address of the local host'''
        # http://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("gmail.com",80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception as e:
        return getNetworkIp()

def host2int(host):
    """

    :param host:
    :return:
    """
    return tuple(map(int, socket.gethostbyname(host).split('.')))



def flush_cache(passwd):
    """
    Flush Linux VM caches. Useful for doing meaningful tmei measurements for
    NetCDF or similar libs.
    Needs sudo password
    :return: bool, True if success, False otherwise
    """
    logger.debug('Clearing the OS cache using sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches')
    #ret = os.system('echo %s | sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"' % passwd)
    ret = os.popen('sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"', 'w').write(passwd)
    return not bool(ret)

def timeit(func=None,loops=1,verbose=False, clear_cache=False, sudo_passwd=None):
    #print 0, func, loops, verbose, clear_cache, sudo_passwd
    if func != None:
        if clear_cache:
            assert sudo_passwd, 'sudo_password argument is needed to clear the kernel cache'

        def inner(*args,**kwargs):
            sums = 0.0
            mins = 1.7976931348623157e+308
            maxs = 0.0
            logger.debug('====%s Timing====' % func.__name__)
            for i in range(0,loops):
                if clear_cache:
                    flush_cache(sudo_passwd)
                t0 = time.time()
                result = func(*args,**kwargs)
                dt = time.time() - t0
                mins = dt if dt < mins else mins
                maxs = dt if dt > maxs else maxs
                sums += dt
                if verbose == True:
                    logger.debug('\t%r ran in %2.9f sec on run %s' %(func.__name__,dt,i))
            logger.debug('%r min run time was %2.9f sec' % (func.__name__,mins))
            logger.debug('%r max run time was %2.9f sec' % (func.__name__,maxs))
            logger.info('%r avg run time was %2.9f sec in %s runs' % (func.__name__,sums/loops,loops))
            logger.debug('==== end ====')
            return result

        return inner
    else:
        def partial_inner(func):
            return timeit(func,loops,verbose, clear_cache, sudo_passwd)
        return partial_inner



@timeit
def blowup(input_array=None,dim1_fact=None, dim2_fact=None):
    assert input_array.ndim == 2, f'input_array has to be 2 dimensional'
    return np.kron(input_array, np.ones((dim1_fact, dim2_fact))).view(input_array.dtype)

@timeit
def scale(input_array, scale_dim1, scale_dim2):     # fill A with B scaled by k
    n, m = input_array.shape
    Y = n * scale_dim1
    X = m * scale_dim2
    A = np.empty(shape=(Y, X), dtype=input_array.dtype)
    for y in range(0, scale_dim1):
        for x in range(0, 2):
            A[y:Y:scale_dim1, x:X:scale_dim2] = input_array
    return A