#import nsidc_download
#reload(nsidc_download)
from nsidc_download import cmr_search, cmr_download



short_name = 'HMA_MAR3_5'
version = '1'
time_start = '2000-01-01T00:00:00Z'
time_end = '2016-12-31T23:59:59Z'
polygon = ''
filename_filter = '*'





def main():

    # Supply some default search parameters, just for testing purposes.
    # These have to be assigned above.
    global short_name, version, time_start, time_end, polygon, filename_filter
    
    urls = cmr_search(short_name, version, time_start, time_end,
                      polygon=polygon, filename_filter=filename_filter)

    cmr_download(urls,'Data/MAR')


if __name__ == '__main__':
    main()
