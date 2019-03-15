import asyncio
import struct
from pyadde import util
import async_timeout
import io
import os
import gzip
import datetime
import typing
import traceback
from pyarea.directory import area_directory
from pyarea.file import AreaFile
import logging




_, n = os.path.split(os.path.abspath(__file__))

logger = logging.getLogger(n)


ADDE_READ_CHUNK_SIZE = 100*1024
VALID_COORD_TYPES = 'A', 'I', 'E'


VALID_COORD_POS = 'U', 'C'



class AddeClient():

    __excluded_arg_names__ = 'self',

    def __init__(self, host=None, port=112,  #server related args
                 #image related args

                 trace=0, version=1,#debug args
                 project=0, user='XXXX', password='',  #auth args
                 conn_timeout=5, adir_timeout=10, aget_timeout= 600# timeout args

                 ):

        assert host not in [None,''], f'invalid host {host}'
        self.reader = self.writer = None
        self.generic_args = {}
        for aname, avalue in locals().items():
            if aname in self.__excluded_arg_names__:
                continue
            setattr(self,aname, avalue)
        for n in  'version', 'trace':
            self.generic_args[n] = getattr(self, n)
        self._binary_content_ = None


    async def __aenter__(self):
        self.content = self._parse_pubsrv_response_(await self.binary_content())
        self.groups = self._extract_groups_(self.content)
        self.group_names = tuple([e[0] for e in self.groups])
        #self.sat_bands = await self.satbands()

        return self




    async def __aexit__(self, exc_type, exc_val, exc_tb):
        #return await self.close()
        pass #because the server does not reuse the socket, the requests are made inside one function from scratch every time, that means they are closed theer as well


    async def close(self):
        if self.reader:
            if not self.reader.at_eof():
                self.reader.feed_eof()

        if self.writer:
            logger.debug(f'Closing connection to {self.host}')
            self.writer.close()

    def json_content(self):
        import json
        d = {'server': self.host}
        glist = []
        for gname, gformat in self.groups:
            descr = self._extract_descriptors_(group_name=gname)
            glist.append({gname:{'format':gformat,'descriptors':dict(descr)}})

        d['groups'] = glist
        return json.dumps(d,indent=4)

    def _create_bin_req_(self, req_type=None, req_text=None, ): #project: 1234, 6999, 0, user: 'XXXX
        assert req_type is not None, 'invalid req type'
        logger.debug(f'creating bin req "{req_type} {req_text}" to {self.host}')

        prj = int(self.project)


        local_ip_int = [127, 0, 1, 1]


        server_ip_int = util.host2int(self.host)

        version = struct.pack('>i', 1)

        server_ip_bstr = struct.pack('4B', *server_ip_int)

        client_ip_bstr = struct.pack('4B', *local_ip_int)

        server_port_bstr = struct.pack('>i', self.port)

        service_type_bstr = struct.pack('>4s', req_type.encode('ascii'))

        preamble = version + server_ip_bstr + server_port_bstr + service_type_bstr

        user = self.user.ljust(4)

        user_bstr = struct.pack('>4s', user.encode('ascii'))
        project_bstr = struct.pack('>i', prj)
        password_bstr = struct.pack('12B', *[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

        lreq = len(req_text)

        nbytes_req = struct.pack('>i', lreq)

        nbinary_bytes = 0  # this is for point data wich I do not use
        if lreq > 120:  # 120 is the standard size  of req. Here the req is larger
            req_text_bstr = req_text
            nbinary_bytes_str = struct.pack('>i', lreq + nbinary_bytes)
            nzeros = 116
            zeros = [0] * nzeros
            zeros_bstr = struct.pack('%dB' % nzeros, *zeros)

            req_body = server_ip_bstr + server_port_bstr + client_ip_bstr + user_bstr + project_bstr + password_bstr + service_type_bstr + nbinary_bytes_str + nbytes_req + zeros_bstr + req_text_bstr.encode('ascii')
        else:  # the req is smaller
            req_text_bstr = req_text.ljust(120, ' ')
            nbinary_bytes_str = struct.pack('>i', nbinary_bytes)
            req_body = server_ip_bstr + server_port_bstr + client_ip_bstr + user_bstr + project_bstr + password_bstr + service_type_bstr + nbinary_bytes_str + req_text_bstr.encode('ascii')

        return preamble + req_body

    async def _query_server_(self, req_type=None, req_text=None, read_in_chunks=False, timeout=None):

        bin_req  = self._create_bin_req_(req_type=req_type, req_text=req_text)

        try:
            '''
            ADDE protocol is stateless 100%, that simply menas that after servicing a goven request the rerver
            sends an RST and closes down. No other fancy stuff. Thsi is whi the next to lines are here and not in __init__ and __senter__
            
            '''
            self.con = asyncio.open_connection(host=self.host, port=self.port)
            self.reader, self.writer = await asyncio.wait_for(self.con, timeout=self.conn_timeout)
            logger.debug(f'New connection to {self.host} was opened')
            with io.BytesIO() as total_data:

                try:
                    # send binary req to server
                    self.writer.write(bin_req)

                    # flush
                    await self.writer.drain()
                    with async_timeout.timeout(timeout=timeout):
                        if not read_in_chunks:

                            data = await self.reader.read()
                            # # read at once
                            total_data.write(data)
                        else:
                            # read in chunks
                            while True:
                                data = await self.reader.read(ADDE_READ_CHUNK_SIZE)
                                if not data:  # client is diconneted
                                    break
                                total_data.write(data)
                    dl = total_data.tell()

                except TimeoutError:
                    logger.error(f'{req_type} req to {self.host} has timed out!')
                    raise
                except Exception as eee:
                    logger.error(f'Exception {eee.__class__.__name__}: {str(eee)} occured in {req_type} ')
                    raise

                if self.port == 112:

                    try:
                        total_data.seek(0)
                        decf = gzip.GzipFile(fileobj=total_data, mode='rb', )
                        data = decf.read()
                        ucdl = len(data)
                        logger.debug(
                            f'server {self.host} has sent {dl} compressed bytes decompressed into {ucdl} bytes  {ucdl/2**20} MB')
                        return data
                    except IOError:
                        total_data.seek(0)
                        v = total_data.getvalue()

                        if len(v) > 1000:
                            v = v[:1000]
                        logger.error(f'Failed to unzip the response content {v} of adir request')
                        raise

        except (asyncio.TimeoutError, ConnectionRefusedError) as re:
            logger.error(f'Issues ({re}) connecting to host {self.host}')
            raise

        except Exception as e:
            logger.error(e)
            raise
        finally:
            await self.close()





    def _parse_pubsrv_response_(self, bin_response=None):

        """
        Parse the result of the response for PUBLIC.SRV request from the ADDE server
        ADDE feaures security through obscurity.
        However, it seems there is a way to find out what does a server provide by asking for the content of PUBLIC.SRV
        text file. This file is similar to RESOLV.SRV which is the config file for MCIDASX ADDE service on a McIDASX installation
        There are peculiarities about parsing a response from an ADDE req, but this request
        should contain text structured as such (i had to inspect the file manually)
        N1 is the ADDE group name
        N2 is the ADDE descriptor name
        TYPE is the data type
        K is the data format or kind
        R1 is the beginning dataset position number
        R2 is the ending dataset position number
        C is the comment field displayed with DSINFO



        :param bin_response: the input decompressed binary response
        :param data_type: str, the data type serverd by ADDE (IMAGE, TEXT, GRID, NAV, POINT, etc) see what can McIDAS/ADDE serve
        :param kind: str, the format or type of the data type (GVAR, AREA, GEOTIFF, HDF)
        :return: an iterable holding dictionaries for every line in response
        """
        #the txtg command is not documented. It looks like if it's decompressed lenght is 96 it is an error.
        #also is the first 2 byres are 4 and 1 it is valid

        # first read the first 8 bytes and interpret as 2 ints
        offset = 8
        step = 4
        # the second byte is not used, I do not know why, this is why I call it spare
        nbytes, spare = struct.unpack('>2i', bin_response[:offset])


        if nbytes == 0 or len(bin_response)==96:  # something went wrong
            msg = ''.join([e for e in bin_response[12:12 + 72].decode('utf-8') if e.isalpha() or e == ' '])
            #msg = data_in[12:12 + 72]
            raise Exception(msg)  # should still be 96 bytes with the error

        lines = []
        while nbytes > 0:
            # extract the number of bytes to read
            t_bytes = struct.unpack('>i', bin_response[offset:offset + step])[0]


            # account foir the previous reading by incrementing offset
            offset += step
            # set nbytes to what we have read
            nbytes = t_bytes
            # read the nbytes from response and parse them as string . These string consists of comma separated key=values pairs terminated by a 0.

            txt = struct.unpack('>%ds' % t_bytes, bin_response[offset:offset + nbytes])[0]
            if txt:
                pairs = txt.decode('ascii').split(',')  # a pair is a string that contains the "=" str character and the left side is the key while the right side is the value. However, it is possible that the value

                try:
                    line_dict = dict()
                    for e in pairs:
                        n_eq_chr = e.count('=')
                        if n_eq_chr == 0:
                            continue  # ignore the element
                        elif n_eq_chr == 1:  # collect
                            k, v = e.split('=')
                            line_dict[k] = v
                        else:
                            feq = e.find('=')
                            k = e[:feq]
                            v = e[feq + 1:]
                            line_dict[k] = v
                except:
                    pass

                if line_dict:
                    lines.append(line_dict)

            offset += nbytes

        return tuple(lines)


    def _parse_adir_response_(self, bin_response=None):
        """
        Parses a response of  an adir request
        Can contain one or more AreaDirectories objects
        The resposne is structured as follows:
            4-byte value containing the total number of bytes of data for the directory being sent (260 + (80 * NumberOfComments))
            4-byte value containing the file number for this image directory
            256-byte image directory
            Comment cards, blank padded to 80 characters
        :param bin_response: binary str
        :return: a list of AreaDirectory objects

        """

        dlen = len(bin_response)-8
        AD_DIRSIZE = 64*4
        offset = 8


        numbytes, areanumber = struct.unpack('!2i', bin_response[:8])

        dirs = list()

        n = 0
        j = 0
        if numbytes == 0:  # something went wrong
            msg = ''.join([e for e in bin_response[12:12 + 72].decode('utf-8') if e.isalpha() or e == ' '])
            raise Exception(msg)  # should still be 96 bytes with the error


        while n < dlen - numbytes:
            start = n + offset
            end = start + AD_DIRSIZE

            adir_bytes = bin_response[start:end]


            ad = area_directory(dir_bytes=adir_bytes)

            if ad.comment_count > 0:
                # cards hold info that can be eventually used (lat lon, res) or just some other staff maybe even bogus
                csize = ad.comment_count*80
                cbytes = bin_response[end:end+csize]

                ad.add_comments(comment_bytes=cbytes)

            t = AD_DIRSIZE + offset + csize

            dirs.append(ad)
            n += t
            j += 1
        dirs.sort(key=lambda adir: adir.nominal_time)
        return dirs

    def _parse_aget_response_(self, bin_response=None):
        offset = 4
        numbytes = int.from_bytes(bin_response[:4],'big')

        if numbytes == 0:  # something went wrong
            msg = ''.join([e for e in bin_response[12:12 + 72].decode('utf-8') if e.isalpha() or e == ' '])
            raise Exception(msg)  # should still be 96 bytes with the error
        else:
            return AreaFile(source=bin_response[4:])




    def _extract_groups_(self, only_image=True):
        """
        Extract the ADDE groups from an iterator of dictinaries generated by parse_pubsrv_response from a PUBLIC.SRV text file
        :param parsed_lines: iter of dicts
        :return: iter(str) with groups
        """
        if only_image:
            return tuple(set([(d['N1'], d['K']) for d in self.content if
                              ('N1' in d.keys() and 'N2' in d.keys() and 'TYPE' in d.keys()) and d['TYPE'] == 'IMAGE']))
        else:
            return tuple(set([(d['N1'], d['K'] )for d in self.content if
                              ('N1' in d.keys() and 'N2' in d.keys() and 'TYPE' in d.keys())]))

    def _extract_descriptors_(self, group_name=None, only_image=True):
        """
        Extract the ADDE groups from an iterator of dictionaris generated by parse_pubsrv_response from a PUBLIC.SRV text file
        :param parsed_lines: iter of dicts
        :return: iter(str) with IMAGE descriptors for a the supplied group
        """
        assert group_name, 'The group argument can not be None or ""'
        groups = self._extract_groups_( only_image=only_image)
        assert group_name in [e[0] for e in groups], 'The group {0} does not exist on ADDE server. Existing groups are {1}'.format(group_name, str(groups))
        if only_image:
            return tuple(set([(d['N2'], d['C'] if 'C' in d else '') for d in self.content if
                              ('N1' in d.keys() and 'N2' in d.keys() and 'TYPE' in d.keys()) and (
                                      d['N1'].lower() == group_name.lower() and d['TYPE'] == 'IMAGE')]))
        else:
            return tuple(set([(d['N2'], d['C'] if 'C' in d else '') for d in self.content if
                              ('N1' in d.keys() and 'N2' in d.keys()  and 'TYPE' in d.keys()) and (
                                      d['N1'].lower() == group_name.lower())]))


    async def binary_content(self):
        if self._binary_content_ is None:
            grp = descr = 'null'
            req_type = 'txtg'
            text_req = f'{grp} {descr} FILE=RESOLV.SRV'

            self._binary_content_ = await self._query_server_(req_type=req_type, req_text=text_req)
            if  self._binary_content_ is None:
                text_req = f'{grp} {descr} FILE=PUBLIC.SRV'
                self._binary_content_ = await self._query_server_(req_type=req_type, req_text=text_req)


        return self._binary_content_

    async def satbands(self):
        grp = descr = 'null'
        req_type = 'txtg'
        text_req = f'{grp} {descr} FILE=SATBAND VERSION=1'
        bin_response = await self._query_server_(req_type=req_type, req_text=text_req)

        numbytes = struct.unpack('!1i', bin_response[:4])[0]


        if numbytes == 0:  # something went wrong
            msg = ''.join([e for e in bin_response[12:12 + 72].decode('utf-8') if e.isalpha() or e == ' '])
            raise Exception(msg)  # should still be 96 bytes with the error

        offset =8
        step = 4
        ns =[]
        l = None
        nbytes = numbytes
        while nbytes > 0:
            t_bytes = struct.unpack('>i', bin_response[offset:offset + step])[0]
            # account foir the previous reading by incrementing offset
            offset += step
            # set nbytes to what we have read
            nbytes = t_bytes
            # read the nbytes from response and parse them as string . These string consists of comma separated key=values pairs terminated by a 0.
            f = f'>{nbytes}s'

            txt = struct.unpack(f, bin_response[offset:offset + nbytes])[0].decode('utf-8')[:-1]

            if txt.startswith('Sat'):
                l = []
            if txt.endswith('EndSat'):
               v = '\n'.join(l)
               #print(v)
               ns.append(v)
               l = None
            if l is not None:
                l.append(txt)
            offset += nbytes

        cont = ''.join(ns)



    def _compose_adir_req_text_(self,
                              group=None, descriptor=None, position=None,
                              band=None, day=None, stime=None, etime=None,
                              aux=None):
        """
        creates a string representing an adir request from a set of options(kwargs)
        :param group: str, the ADDE group
        :param descriptor: str, the ADDE descriptor
        :param position: str position inside the dataset, can be ALL, x,  a string representing two ints
        position can be negative, relative or absolute. If negative(it si also relative) ex -5 there will be 5 headers returned, if positive it is absolute and either a sepcific position can be requested or a range of positions
        :param band: int or str 'startband endband' or text 'ALL'
        :param stime: str, start time , ex 14:00 (%H:%M)
        :param etime: str, end time, ex 15:00 (%H:%M)
        :param day: str, day of the image(nominal) 2017-05-01 (%Y-%m-%d)
        :param aux: str, YES or NO, if YES extra calib data is included in comments
        :param version: int, defaults to 1
        :return:
        """
        # check the mandatory
        assert group is not None, f'group  can not be {group}'
        assert descriptor is not None, f'descriptor can not be {descriptor}'
        assert position is not None, f'position can not be {position}'
        assert group in self.group_names, f'Invalid group {group}. Valid groups on {self.host} are {self.group_names}'
        valid_descr = tuple([e[0] for e in self._extract_descriptors_(group_name=group)])
        assert descriptor in valid_descr, f'Invalid descpritor {descriptor} for group {group}. Valid descriptors are {valid_descr}'
        del valid_descr
        assert band is not None, f'Invalid band {band}'




        try:
            int(band)
        except Exception as e:

            if isinstance(band, str):
                pass

            elif isinstance(band, typing.Iterable):
                band = ' '.join(map(str, band))


        position = str(position)


        if stime:
            if not etime:
                time = f'{stime} {stime}'
            else:
                time = f'{stime} {etime}'


        if position == '':
            lposition = '0 0'
        elif len(position) > 1:
            if position.lower() == 'all':
                lposition = 1095519264  # ALL in binary int 32
            elif position.lower() == 'x':
                lposition = 'X X'
            else:
                try:
                    posval = int(position)
                    if posval < 0:
                        lposition = position + ' 0'
                    else:
                        lposition = position  # + ' ' + position
                except Exception as e:
                    try:
                        position = eval(position)
                        if isinstance(position, typing.Iterable):
                            lposition = ' '.join(map(str, position))
                        else:
                            lposition = str(position)

                    except Exception as e:

                        lposition = str(position)


        else:
            lposition = position

        pos_str = f'{group} {descriptor} {lposition} '

        extra_args = list()
        extra_args_dict  = dict()

       #update generic args from __init__
        extra_args_dict.update(self.generic_args)

        # collect
        for e, ev in sorted(extra_args_dict.items()):
            if ev is not None:
                extra_args.append(f'{e.upper()}={str(ev).upper()}' )

        extra_args_str = ' '.join(extra_args) or ''

        band_str = f'BAND={band} ' if band else ''
        day_str = f'DAY={day} ' if day else ''
        time_str = f'TIME={time} ' if day else ''
        aux_str = f'AUX={aux} ' if aux else ''
        req_text = f'{pos_str}{band_str}{day_str}{time_str}{aux_str}{extra_args_str}'

        return req_text

    async def adir(self,
                         group=None, descriptor=None, position=None,
                         band=None, day=None, stime=None, etime=None,
                         aux=None,

                   ):
        """
        Request image headers.
        The results are insances  of area directory objects for every found image
        https://www.ssec.wisc.edu/mcidas/doc/prog_man/2015/servers-5.html#25171

        :param group: str, group name
        :param descriptor: str, descr name
        :param position: int or iter of ints or str, beginning and end position file number
                can be the value ALL; positive numbers represent absolute locations; negative numbers are time-relative offsets (no default)
                The client tries to make the best out of this keyword and is forgiving
        :param band: int or str=ALL, band number
        :param day: day range to search, str, ccyyddd or yyddd or yyyy-mm-dd
        :param stime: start time for the time range to search, str eg hh:mm:ss
        :param etime: end time for the time range to search, str eg hh:mm:ss
        :param aux: str, YES or NO (defaylt=NO) inclide extra calib info in comments

        :return: an iterable with area_directory objects
        """

        req_text = self._compose_adir_req_text_(

                                        group=group, descriptor=descriptor, position=position,
                                        band=band,day=day, stime=stime, etime=etime, aux=aux


        )



        bin_resp = await self._query_server_(req_type='adir', req_text=req_text, timeout=self.adir_timeout)


        adirs = self._parse_adir_response_(bin_response=bin_resp)

        # all the other
        extra_args = list()
        for e, ev in sorted(self.generic_args.items()):
            if ev is not None:
                extra_args.append(f'{e.upper()}={str(ev).upper()}')
        # extra_args.append(f'NAV=X')
        extra_str = ' '.join(extra_args) or ''
        for adir in adirs:
            sts = adir.start_time.time()
            adir.text_request = f'adde://{self.host}/adir/{group} {descriptor} BAND={adir.bands[0]} DAY={day} TIME={sts} {sts} {extra_str}'
        return adirs



    def _compose_aget_req_text_(self,

                                group=None, descriptor=None, position=None,  # dataset args
                                coord_type=None, coord_pos=None, coord_start_dim1=None, coord_start_dim2=None,  # coord args
                                nlines=None, nelems=None,  # image props
                                day=None, stime=None, etime=None,
                                band=None, unit=None, spac=None, cal=None,
                                lmag=None, emag=None, doc=None, aux=None

                                ):


        # first position
        group = str(group)
        descriptor = str(descriptor)
        position = str(position)




        dataset_str = f'{group} {descriptor} {position} '
        #mag business is tricky
        #form the docs
        #blow ups are done on the client to conserve transmission bandwidth (default=1);
        # values must be integers; negative numbers mean a blowdown must be performed
        #in order to be prefectly accurate the clent should handle blow-up.!!!!
        #TODO handle blowup . I guess blo

        if lmag is not None:
            lmagi = int(lmag)
            if lmagi < 0:
                nlines = nlines // abs(lmagi)  # seems like whatever the sign of mag the aget does only downsampling??
            else: # let's ignore pos mag aka blowup.
                #nlines = nlines * abs(lmagi)

                lmag = 1


        if emag is not None:
            emagi = int(emag)
            if emagi < 0:
                nelems = nelems // abs(emagi)  # seems like whatever the sign of mag the aget does only downsampling??
            else:
                #nelems = nelems * abs(emagi)
                emag = 1

        # create the LOCATE from coord_type, coord_pos, coord_start_dim1, coord_start_dim2, nlin, nele
        coord_str = f'{coord_type}{coord_pos} {coord_start_dim1} {coord_start_dim2} X {nlines} {nelems} '

        band_str = f'BAND={band} '

        day_str = f'DAY={day} ' if day else ''
        if stime is not None and etime is not None:
            stime_str = stime or ''
            etime_str = etime or ''
            setime_str = f'{stime_str} {etime_str}'
            time_str = f'TIME={setime_str} '
        else:
            time_str = ''

        # mag
        lmag_str = f'LMAG={lmag} ' if lmag else ''
        emag_str = f'EMAG={emag} ' if emag else ''

        unit_str = f'UNIT={unit} ' if unit else ''
        spac_str = f'SPAC={spac} ' if spac else ''
        cal_str = f'CAL={cal} ' if cal else ''

        aux_str = f'AUX={aux} ' if aux else ''
        doc_str = f'DOC={doc} 'if doc else ''

        extra_args = list()

        # all the other
        for e, ev in sorted(self.generic_args.items()):
            if ev is not None:
                extra_args.append(f'{e.upper()}={str(ev).upper()}')
        #extra_args.append(f'NAV=X')
        extra_str = ' '.join(extra_args) or ''


        req_text = f'{dataset_str}{coord_str}{lmag_str}{emag_str}{band_str}{day_str}{time_str}{unit_str}{spac_str}{cal_str}{doc_str}{aux_str}{extra_str}'


        return req_text

    async def aget(self,
                   group=None, descriptor=None, position=None,  #dataset args
                   coord_type='A', coord_pos='U', coord_start_dim1=None, coord_start_dim2=None,  #coord args
                   nlines=None, nelems=None,   #image props
                   day=None, stime=None, etime=None,
                   band=None, unit=None, spac='X', cal='X',
                   lmag=1, emag=1, doc='YES', aux='YES',



                   ):
        """
        Request image data. The result is an instance of McIDAS AREA file
        https://www.ssec.wisc.edu/mcidas/doc/prog_man/2015/servers-5.html#41467
        :param
        :param group: str, group name
        :param descriptor: str, descriptor
        :param position: int or iter of ints or str, beginning and end position file number
                can be the value ALL; positive numbers represent absolute locations; negative numbers are time-relative offsets (no default)
                The client tries to make the best out of this keyword and is forgiving
        :param coord_type: coordinate type, E)arth, (I)mage (original coord system, (A)rea;
        :param coord_pos: str (C)entered or (U)pper; no default)
                This arg is used with the next two parameters; for example,
                to return an image centered at latitude 43.5 and longitude 90.0, the client request will contain EC 43.5 90.0
                to return the whole image the client requets will contain AU 0 0 X nline nelems
        :param coord_start_dim1: lat or line number
        :param coord_start_dim2: lon or emement number
        :param nlines: int, number or image lines to transmit
        :param nelems:int, number of elements to transmit
        :param day: day range to search, str, ccyyddd or yyddd or yyyy-mm-dd
        :param stime: start time for the time range to search, str eg hh:mm:ss
        :param etime: end time for the time range to search, str eg hh:mm:ss
        :param aux: str, YES or NO (defaylt=NO) inclide extra calib info in comments
        :param band: int or string 'ALL
        :param unit: str, calibration type requested
        :param spac: number of bytes per data point, default=wtored in format
        :param cal: ???
        :param lmag: line magnification factor, blow ups are done on the client to conserve transmission bandwidth (default=1); values must be integers; negative numbers mean a blowdown must be performed
        :param emag: element magnification factor, blow ups are done on the client to conserve transmission bandwidth (default=1) values must be integers; negative numbers mean a blowdown must be performed
        :param doc: if YES, include the line documentation block default on server=NO
        :param aux: if YES, additional calibration information is sent, default on server=NO
        :return: an instance of McIDAS Area file object

        ADDE server features inconsistent behaviour for band keyword
        in adir the band can be ALL, in or two ints. When the band is expreseed as 2 numbers the server will return
        a directory for all bands in the range (band_start, band_end)
         In aget, whe the band is expressed as two numbers the server will understand the bands as unique so
        it will return allways two bands, the start band and the end band

        #is three numbers are
        """
        #some of the args will be handled here while others in _compose_aget_req_text_.

        # sanity check
        assert group is not None, f'group  can not be {group}'
        assert descriptor is not None, f'descriptor can not be {descriptor}'
        assert position is not None, f'position can not be {position}'
        assert group in self.group_names, f'Invalid group {group}. Valid groups on {self.host} are {self.group_names}'
        valid_descr = tuple([e[0] for e in self._extract_descriptors_(group_name=group)])
        assert descriptor in valid_descr, f'Invalid descpritor {descriptor} for group {group}. Valid descriptors are {valid_descr}'
        del valid_descr

        assert coord_type not in ['', None], f' Invalid coord_type {coord_type}'
        assert coord_type in VALID_COORD_TYPES, f'Invalid coord_type {coord_type}. Valid values are {VALID_COORD_TYPES}'
        assert coord_pos not in ['', None], f'Invalid coord_pos {coord_pos}'
        assert coord_pos in VALID_COORD_POS, f'Invalid coord_pos {coord_pos}. Valid values are {VALID_COORD_POS}'

        assert band is not None, f'Invalid band {band}'

        # the position here is handled in a different way thea adir

        try:
            position = int(position)

        except Exception as e:
            raise Exception(f'Position {position} is invalid for an adir request. Position should be a number.')

        assert band is not None, f'Invalid band {band}'

        try:
            int(band)
        except Exception as ve:

            if isinstance(band, str):
                if len(band) > 1:
                    try:
                        bs, be = band.split(' ')
                        try:
                            int(bs)
                            int(be)

                        except Exception as e:
                            raise Exception(f'Invalid band {band}. In the string form band can be "ALL" or a "start end" ')

                    except Exception as e:
                        if not band == 'ALL':
                            pass
                            #raise Exception(f'Invalid band {band}')

            elif isinstance(band, typing.Iterable):
                # if len(band) != 2:
                #     raise Exception(
                #         f'Invalid band argument {band}. When band is iter it has to have 2 elements: start_band end_band')
                band = ' '.join(map(str, band))

        assert day is not None, f'Invalid day {day}'
        assert ':' in stime, f'Invalid stime {stime}'
        assert ':' in etime, f'Invalid etime {etime}'

        #nlines and nelems
        if nlines is None:
            if nelems is not None:
                raise Exception(f'Invalid arg neleems {nelems}. When nlines is  supplied nelems needs to be as well')
        if nelems is None:
            if nlines is not None:
                raise Exception(f'Invalid arg nlines {nlines}. When nelems is supplied nlines needs to be as well')
        try:
            nlines = int(nlines)
        except Exception as e:
            pass
        try:
            nelems = int(nelems)
        except Exception as e:
            pass

        #coord_start and coord_end
        if coord_start_dim1 is None:
            if coord_start_dim2 is not None:
                raise Exception(
                    f'Invalid arg coord_start_dim2 {coord_start_dim2}. When coord_start_dim1 is  supplied coord_start_dim2 needs to be as well')
        if coord_start_dim2 is None:
            if coord_start_dim1 is not None:
                raise Exception(
                    f'Invalid arg coord_start_dim1 {coord_start_dim1}. When coord_start_dim2 is supplied coord_start_dim1 needs to be as well')

        #after these it is endured the doord_start
        adir = None
        if nlines is None and nelems is None: # make an adir req  and use the size of the image, This usualy means the aget was invoked directly
            # and no adir was issued. in this case it is OK to make an adir req
            #adir will throw exceptions in case it get no data..

            adirs = await self.adir(
                                group=group, descriptor=descriptor, position=position, band=band,
                                day=day,stime=stime, etime=etime, aux='YES' #overwrite aux
                              )

            if len(adirs)> 1:
                sd = sorted(adirs, key=lambda d: d.lines*d.elements, reverse=False )
                adir = sd[0]

            else:
                adir = adirs[0]
            nlines, nelems = adir.size
            logger.info(f'Image size is {adir.size}')

            #nlines and nelems should be adujsted as per mag only in case they were not supplied.


        #handle coords
        if coord_type == 'E':
            if coord_pos == 'C':
                if coord_start_dim1 is None and coord_start_dim2 is None: #user supplied nothing let's use the header
                    if adir is not None:
                        coord_start_dim1, coord_start_dim2 = adir.ssp
                    else:
                        raise Exception(f'Please suply args coord_start_dim1 and coord_start_dim2 (latlon coordinates center of the image)')

            if coord_pos == 'U':

                if coord_start_dim1 is None and coord_start_dim2 is None:
                    # user supplied nothing , image nav could be be used to compute the upper eft corenr latlon.
                    #however thi is not feasible because of GEOS proejction
                        raise Exception(
                            f'Please suply args coord_start_dim1 and coord_start_dim2 (latlon coordinates upper left corner of the image)')

        if coord_type == 'A': # easier at leats for me
            if coord_pos == 'C':
                if coord_start_dim1 is None and coord_start_dim2 is None:  # user supplied nothing let's use the header
                    if adir is not None:
                        coord_start_dim1 = adir.lines //2
                        coord_start_dim2 = adir.elements //2

                    else:
                        raise Exception(
                            f'Please suply args coord_start_dim1 and coord_start_dim2 (area coordinates center of the image)')
            if coord_pos == 'U':
                if coord_start_dim1 is None and coord_start_dim2 is None:
                    coord_start_dim1 = coord_start_dim2 = 0

        if coord_type == 'I':
            if coord_pos == 'C':
                if coord_start_dim1 is None and coord_start_dim2 is None:
                    if adir is not None:
                        start_line, end_line, start_elem, end_elem = adir.imgbox
                        coord_start_dim1 = (end_line-start_line)//2
                        coord_start_dim2 = (end_elem-start_elem)//2
                    else:
                        raise Exception(
                            f'Please suply args coord_start_dim1 and coord_start_dim2 (image coordinates center of the image)')
            if coord_pos == 'U':
                if coord_start_dim1 is None and coord_start_dim2 is None:
                    if adir is not None:
                        coord_start_dim1 = adir.line_ul
                        coord_start_dim2 = adir.element_ul
                    else:
                        raise Exception(
                            f'Please suply args coord_start_dim1 and coord_start_dim2 (image coordinates upper left corenr of the image)')


        #unit
        if unit is None:
            if adir is not None:
                unit = adir.get_units()
        spac = spac or 'X'
        cal = cal or 'X'



        req_text = self._compose_aget_req_text_(
            group=group, descriptor=descriptor, position=position,
            coord_type=coord_type, coord_pos=coord_pos, coord_start_dim1=coord_start_dim1, coord_start_dim2=coord_start_dim2,
            nlines=nlines, nelems=nelems,
            day=day, stime=stime, etime=etime, band=band,
            unit=unit, spac=spac, cal=cal,
            lmag=lmag, emag=emag, doc=doc, aux=aux

        )



        bin_resp = await self._query_server_(req_type='aget', req_text=req_text, timeout=self.aget_timeout,read_in_chunks=False)

        return self._parse_aget_response_(bin_response=bin_resp)






async def test(host=None,):
    try:
        d = (datetime.datetime.now() -datetime.timedelta(days=1)).date()
        d = datetime.datetime.now().date()


        async with AddeClient(host=host,) as c:

            try:
                print(c.json_content())
                #print (c.group_names)

                # adir_resp = await c.adir( group='RTGOESR', descriptor='RADFM3C07',position='ALL',
                #                     band=7,day=d, stime='00:00', etime='23:59', aux='YES'
                #
                #)

                # adir_resp = await c.adir( group='GVAR', descriptor='ALL',position=0,
                #                      band=[3,4],day=d, stime='01:00', etime='02:00', aux='NO'
                #
                # )
                # adir_resp = await c.adir( group='PUB', descriptor='GWNHEM04I2',position='ALL',
                #
                #                      band=[3,4],day='2018-06-17', stime='01:00', etime='02:00', aux='YES'
                #
                # )
                #return adir_resp

                # area_file = await c.aget(
                #      group='RTGOESR', descriptor='FD', position=0,  # dataset args
                #      coord_type='I', coord_pos='C', #coord_start_dim1=0, coord_start_dim2=0,  # coord args
                #      nlines=None, nelems=None, # image props
                #      day=d, stime='01:00', etime='02:00', band='ALL',
                #      unit=None, spac=None, cal=None,
                #      lmag=1, emag=1,
                #      doc='YES', aux='YES',
                #                          )
                # area_file = await c.aget(group='PUB', descriptor='GWNHEM04I2', position='0',  # dataset args
                #                          coord_type='A', coord_pos='U',  # coord_start_dim1=0, coord_start_dim2=0,  # coord args
                #                          nlines=None, nelems=None,  # image props
                #                          band=2,day=d, stime='23:00', etime='23:59',
                #                          unit='RAW',cal='X', lmag=2,emag=2, #spac=2,
                #                          doc='YES', aux='YES', )
                # area_file = await c.aget(group='IMAGEALL', descriptor='GW-39', position='0',  # dataset args
                #                          coord_type='A', coord_pos='U',
                #                          # coord_start_dim1=0, coord_start_dim2=0,  # coord args
                #                          nlines=None, nelems=None,  # image props
                #                          band=2, day=d, stime='23:00', etime='23:59', unit='RAW', cal='X', lmag=1,
                #                          emag=1,  # spac=2,
                #                          doc='YES', aux='YES', )
                # area_file = await c.aget(group='GVAR', descriptor='ALL', position=0,  # dataset args
                #                          coord_type='A', coord_pos='C',
                #                          # coord_start_dim1=0, coord_start_dim2=0,  # coord args
                #                          nlines=None, nelems=None,  # image props
                #                          band=[1,2,4], day=d, stime='23:00', etime='23:59', unit='RAW',# spac=2,
                #                          cal='X', lmag=4, emag=4, doc='YES', aux='YES', )
                # area_file = await c.aget(group='RTGOESR', descriptor='FD', position=0,  # dataset args
                #                          coord_type='I', coord_pos='U',  # coord_start_dim1=0, coord_start_dim2=0,  # coord args
                #                          nlines=None, nelems=None,  # image props
                #                          day=d, stime='01:00', etime='02:00', band=[2],
                #                          unit=None, spac=None, cal=None, lmag=-8,
                #                          emag=-8, doc='YES', aux='YES', )
                #
                # area_file = await c.aget(group='RTGOESR', descriptor='RADFM3C07', position='ALL',  # dataset args
                #                          coord_type='A', coord_pos='U',
                #                          # coord_start_dim1=0, coord_start_dim2=0,  # coord args
                #                          nlines=None, nelems=None,  # image props
                #                          day=d, stime='01:00', etime='02:00', band=[7], unit=None, spac=None, cal=None,
                #                          lmag=1, emag=1, doc='YES', aux='YES', )
                #
                # return area_file

            except Exception as e:
                efp = io.StringIO()
                traceback.print_exc(file=efp)
                message = efp.getvalue()
                logger.error(message)
                return host, e
    except Exception as ee:
        return host, ee



async def collect(hosts=None):
    tasks = list()
    for h in hosts:
        taks = asyncio.ensure_future(test(host=h))
        tasks.append(taks)
    return await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel('DEBUG')
    logger.name = n
    adde_server = 'goeswest.unidata.ucar.edu'
    adde_server = 'goeseast.unidata.ucar.edu'
    #adde_server = 'satepsanone.nesdis.noaa.gov'
    # adde_server = 'satepsdist1e.nesdis.noaa.gov'
    # adde_server = 'tornado.geos.ulm.edu'
    # adde_server = 'adde.ssec.wisc.edu'
    # adde_server = 'OPENARCHIVE.SSEC.WISC.EDU'
    #adde_server = 'adde.eumetsat.int'
    # adde_server = 'atm.ucar.edu'
    adde_server = 'amrc.ssec.wisc.edu'
    # adde_server = 'himawari.ssec.wisc.edu'
    # adde_server = 'fy2g.ssec.wisc.edu'
    # adde_server = 'coms.ssec.wisc.edu'
    # adde_server = 'indoex.ssec.wisc.edu'
    # adde_server = 'KALPANA.SSEC.WISC.EDU'
    # adde_server = 'EASTL.SSEC.WISC.EDU'
    # adde_server = 'weather3.admin.niu.edu'
    adde_server = 'adde.unidata.ucar.edu'
    adde_server = 'geoarc.ssec.wisc.edu'
    hosts = [
        'goeswest.unidata.ucar.edu',
        'lead.unidata.ucar.edu',
        'atm.ucar.edu',
        'motherlode.unidata.ucar.edu',
        'adde.ssec.wisc.edu',
        'satepsanone.nesdis.noaa.gov',
        'geoarc.ssec.wisc.edu',
        'openarchive.ssec.wisc.edu',
        'amrc.ssec.wisc.edu',
        'adde.ucar.edu',
        'adde.unidata.ucar.edu',
        'eastl.ssec.wisc.edu',
        'himawari.ssec.wisc.edu'


    ]
    then = datetime.datetime.now()

    loop = asyncio.get_event_loop()
    try:
        import pylab
        f = collect(hosts=[adde_server])
        #f = collect(hosts=hosts)
        a = loop.run_until_complete(f)
        for e in a:
            try:
                # for band in e.directory.bands:
                #     band_chunk_start = 1 + (band - 1) * 18
                #     # band_chunki_start = ninstr_bytes + (band-1)* n_calib_elements*4
                #     band_chunk_end = band_chunk_start + 18
                #
                #     calib_info = e.cal[band_chunk_start:band_chunk_end]
                #     print(dict(zip(range(1, len(calib_info) + 1), calib_info)))
                d = e.data

                for i, b in enumerate(d):
                    pylab.title(f'Band {e.directory.bands[i]}')
                    pylab.imshow(b)
                    pylab.show()

            except Exception as err:
                if isinstance(err, typing.Iterable):
                    host, em = e
                    logger.info(f'Server {host} says {em}')
                else:
                    print(e)
                    logger.error(err)

    except KeyboardInterrupt as ke:


        def shutdown_exception_handler(loop, context):

            if "exception" not in context or not isinstance(context["exception"], asyncio.CancelledError):
                loop.default_exception_handler(context)


        loop.set_exception_handler(shutdown_exception_handler)
        tasks = asyncio.gather(*asyncio.Task.all_tasks(loop=loop), loop=loop, return_exceptions=True)
        tasks.add_done_callback(lambda t: loop.stop())
        tasks.cancel()
        while not tasks.done() and not loop.is_closed():
            loop.run_forever()
        pending = asyncio.Task.all_tasks()
        for ptask in pending:
            ptask.cancel()

    finally:
        loop.close()
        now = datetime.datetime.now()
        logger.info(f'Total run time {now-then}')
