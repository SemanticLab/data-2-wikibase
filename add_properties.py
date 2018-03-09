#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pywikibot import family
import pywikibot, json, csv, sys

"""
    Notes:
    We need to remove the built in throttling because we 
    are working on our own localhost running wikibase, we don't
    care if we do a ton of requests, we are likely the only user
"""
# over write it
def wait(self, seconds):
    """Wait for seconds seconds.
    Announce the delay if it exceeds a preset limit.
    """
    pass
    # if seconds <= 0:
    #     return

    # message = (u"Sleeping for %(seconds).1f seconds, %(now)s" % {
    #     'seconds': seconds,
    #     'now': time.strftime("%Y-%m-%d %H:%M:%S",
    #                          time.localtime())
    # })
    # if seconds > config.noisysleep:
    #     pywikibot.output(message)
    # else:
    #     pywikibot.log(message)

    
    # time.sleep(seconds)

pywikibot.throttle.Throttle.wait = wait

if __name__ == "__main__":


    if len(sys.argv) == 1:
        raise ValueError('Missing input CSV file')

    csv_path = sys.argv[1]
    csv_file = open(csv_path,'r')

    csv_reader = csv.DictReader(csv_file)

    # If you changed the name of the site to something else make sure to change it here
    site = pywikibot.Site('en', 'semlab')
    site.login()

    complete_data = []

    for row in csv_reader:
        row = dict(row)
        

        # build our API data call for each property
        data = {
            'datatype': row['Datatype'],  # mandatory
            'descriptions': {
                'en': {
                    'language': 'en',
                    'value': row['Property Description']
                }
            },
            'labels': {
                'en': {
                    'language': 'en',
                    'value': row['Property Label']
                }
            }
        }

        params = {
            'action': 'wbeditentity',
            'new': 'property',
            'data': json.dumps(data),
            'summary': 'bot adding in properties',
            'token': site.tokens['edit']
        }

        req = site._simple_request(**params)
        results = req.submit()

        row['PID'] = results['entity']['id']

        print(row['PID'], row['Property Label'])
        complete_data.append(row)

    csv_file.close()

    with open(csv_path+'_updated.csv','w') as out:

        fieldnames = list(complete_data[0].keys())
        writer = csv.DictWriter(out, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(complete_data)


