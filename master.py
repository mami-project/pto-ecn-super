import datetime
import json

from ptocore.analyzercontext import AnalyzerContext
from ptocore.sensitivity import margin
from ptocore.collutils import grouper

def calculate_super_condition(conditions):
    
    if 'ecn.connectivity.works' in conditions:
        return ['ecn.super.connectivity.works']
    
    if len(conditions) == 0:
        print('WHOAAA, this should not happen: len(conditions) == 0')
        return []
    
    super_condition = conditions[0]
    for condition in conditions:
        if condition == super_condition:
            pass
        else:
            super_condition = 'ecn.connectivity.transient'
            break
    
    if super_condition == 'ecn.connectivity.works':
        return ['ecn.super.connectivity.works']
    elif super_condition == 'ecn.connectivity.broken':
        return ['ecn.super.connectivity.broken']
    elif super_condition == 'ecn.connectivity.transient':
        return ['ecn.super.connectivity.transient']
    elif super_condition == 'ecn.connectivity.offline':
        return ['ecn.super.connectivity.offline']

    print('WHOAAA, this should not happen: supercondition of unkown type')
    return []
    
def create_super_observation(db_entry):
    
    conditions = calculate_super_condition(db_entry['conditions'])
    
    dip = db_entry['_id']['dip']
    path = ['*', dip]
    
    timedict = dict()
    timedict['from'] = db_entry['time_from']
    timedict['to'] = db_entry['time_to']
    
    sources = dict()
    sources['obs'] = db_entry['obs']
    
    observation = dict()
    observation['time'] = timedict
    observation['path'] = path
    observation['conditions'] = conditions
    observation['sources'] = sources
    observation['value'] = dict()
    
    return observation


print("--> Good morning! My name is pto-ecn-super")

ac = AnalyzerContext()
OFFSET = datetime.timedelta(hours = 2)
max_action_id, timespans = margin(OFFSET, ac.action_set)

# only analyze one timespan per time
time_from, time_to = timespans[0]
ac.set_result_info(max_action_id, [(time_from, time_to)])

print("--> running with max action id: {}".format(max_action_id))
print("--> running with time from: {}".format(time_from))
print("--> running with time to: {}".format(time_to))

# The observations that we are interested in.
input_types = [
    'ecn.connectivity.works',
    'ecn.connectivity.broken',
    'ecn.connectivity.transient',
    'ecn.connectivity.offline'
]

stages = [
    # Get all valid inputs within the requested timespan
    {
        '$match': {
            # Question to self, are new observations pushed to the frot
            # or back of the array? Is it right to check the zeroth
            # element for validity?
            'action_ids.0.valid': True,
            'conditions': {'$in': input_types},
            'time.from': {'$gte': time_from},
            'time.to': {'$lte': time_to},
            # We are only interested in observations that have a location set.
            'value.location': {'$ne': None}
        }
    },
    # Create a record for every individual observation
    {
        '$unwind': '$conditions'
    },
    # Only keep the observations in our input types, and with a location
    {
        '$match': {
            'conditions': {'$in': input_types}
            }
    },
    # Group by destination ip and location
    {
        '$group': {
            '_id': {'dip': {'$arrayElemAt': ['$path', -1]},
                'location':'$value.location' },
            'sips': {'$push': {'$arrayElemAt': ['$path', 0]}},
            'conditions': {'$push': '$conditions'},
            'obs': {'$push': '$_id'},
            'time_from': {'$min': '$time.from'},
            'time_to': {'$max': '$time.to'}
        }
    },
    # Count the number of source ips
    {
        '$project': {
            'conditions': 1,
            'num_sips': {'$size': '$sips'},
            'sips': 1,
            'obs': 1,
            'time_from': 1,
            'time_to': 1
        }
    }
]

print("--> starting aggregation")
cursor = ac.observations_coll.aggregate(stages, allowDiskUse=True)
print("--> starting insertion in to DB")
for observations in grouper(cursor, 1000):
    for observation in observations:
        ac.temporary_coll.insert_one(create_super_observation(observation))

print("--> Goodnight!")
