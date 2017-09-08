from Cloud import Customer, Location
from Entities import FtpSender
import re
import datetime
import gzip

start_date = datetime.date.today()
end_date = start_date
send = False
impacted_files = []

for token in sys.argv[1:]:
    if 'start_date=' in token:
        start_date = datetime.datetime.strptime(token.replace('start_date=', '').strip(), '%Y-%m-%d').date()
        sys.argv.remove(token)
    if 'end_date=' in token:
        end_date = datetime.datetime.strptime(token.replace('end_date=', '').strip(), '%Y-%m-%d').date()
        sys.argv.remove(token)
    if token == 'send':
        send = True

if end_date < start_date:
    print_error('End date cannot be less than start date')
    exit(1)

dates = [end_date - datetime.timedelta(days=x) for x in range((end_date - start_date).days)]
dates.append(start_date)


wss = Customer('shopwss', '4116bd3a-453c-11e7-9703-40007c4e2622', 'YQ9Xhun9DhoRCvAzAR3ByauY9lk')
wss.load_locations()
stores = Location.filter('location_type', 'store')
#chosen_date = datetime.date.today() - datetime.timedelta(days=1)
for date in dates:
    filename = 'TRAFFIC_%s%s.txt.gz' % (date, datetime.datetime.now().strftime('%H%M%S'))
    the_file = gzip.open(filename, 'wb')
    the_file.write('DATE|HOUR|LOC_NUM|TRAFFIC\n')
    for store in stores.values():
        store.load_data(['traffic_in', 'traffic_out'], chosen_date, granularity=datetime.timedelta(hours=1), storehours=False, to_hour=datetime.datetime.now().time())
        for the_time in sorted(store.data[chosen_date]['traffic_in'].keys()):
            incount = store.data[chosen_date]['traffic_in'][the_time].value
            outcount = store.data[chosen_date]['traffic_out'][the_time].value
            the_file.write('%s|%s|%s|%s\n' % (the_time.strftime('%m/%d/%Y'), the_time.strftime('%H%M%S'), store.store_id,  incount))
    the_file.close()
    impacted_files.append(filename)

if send:
    ftp = FtpSender('sftp wss@ftp.quantisense.com', 'H3k0l2Aq')
    for filename in impacted_files:
        result = ftp.send_file(filename)
        print(result)