from pyadde import util
import numpy as np
import pylab

if __name__ == '__main__':
    import logging
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel('INFO')
    print(util.get_local_ip())
    print(util.getNetworkIp())


    n = 5000
    m = 4000
    k1 = 4
    k2 = 5
    sz = n*m

    a = util.host2int(util.get_local_ip())
    a = np.arange(sz,dtype=np.int32).reshape(n,m)
    print(a.shape)
    # pylab.imshow(a, interpolation='nearest')
    # pylab.title(a.shape)
    # pylab.show()

    c = util.scale(a, k1, k2)
    print(c.shape)
    # pylab.imshow(c, interpolation='nearest')
    # pylab.title(c.shape)
    # pylab.show()

