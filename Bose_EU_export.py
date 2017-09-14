from Cloud import Location, Data, Customer
import pexpect
import datetime

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


bose = Customer('bose', '90aa6948-a742-11e6-9f9a-4000166794ec', 'dOFEz9qz2SlhI5oDa81igi-SFwU')
bose.load_locations()
yesterday = datetime.date.today() - datetime.timedelta(days=1)
stores = Location.filter('location_type', 'store')
eu_stores = Location.filter('full_name', 'Bose -> Europe Stores', dict_to_filter=stores)
for date in dates:
    filename='RetailNext_%s_EU.csv' % str(yesterday).replace('-', '')
    eu_file = open(filename, 'w')
    for store in eu_stores.values():
        Data.load(bose, ['traffic_in', 'traffic_out'], yesterday, yesterday, store, granularity=datetime.time(1))
        times = sorted(store.data[yesterday]['traffic_out'].keys())
        for t in times:
            eu_file.write('%s,%s,%s,%s,%s,\n' % (store.store_id, str(yesterday).replace('-', ''), str(t.time()).replace(':', ''), store.data[yesterday]['traffic_in'][t].value, store.data[yesterday]['traffic_out'][t].value))
    eu_file.close()
    impacted_files.append(filename)

if send:
    command = 'sftp retail_next_ext@sftp.bose.com'
    child = pexpect.spawn(command, timeout=60, env={"TERM": "xterm"})
    child.expect('ftp>')
    child.sendline('cd /incoming/Production/BIPCL010/EUROPE')
    child.expect('ftp>')
    #filename = 'RetailNext_%s_EU.csv' % str(datetime.date.today() - datetime.timedelta(days=1)).replace('-', '')
    for filename in impacted_files:
        child.sendline('put %s' % filename)
        print filename
        child.expect('ftp>')
