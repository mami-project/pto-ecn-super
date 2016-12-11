import datetime
import json

from ptocore.analyzercontext import AnalyzerContext
from ptocore.sensitivity import margin
from ptocore.collutils import grouper

TIMESPAN_HOURS = 2
EXCLUDED_CAMPAIGNS = ['testing', 'testing-import', 'testing-fi']

def verify_all_elements_equal(array_to_check, element_value = None):
    """
    Verifies that all elements in an array are equal to a certain value.

    If no value is passed, the first element of the array is used.

    :param list array_to_check: the array to check the elements from
    :param element_value: the value the elements should be equal to.
        defaults to the first element of the array.
    :returns: True if all elements are equal, False otherwise
    :rtype: bool
    """

    # There is nothing in the array, that's not right
    if len(array_to_check) == 0:
        return False

    # if no value is specified, just check that all values in the array are
    # equal to the first one.
    if element_value == None:
        element_value = array_to_check[0]

    for element in array_to_check:
        if element != element_value:
            return False

    return True

def calculate_super_condition(conditions):
    """
    Combines multiple conditions in to a super condition.

    First looks if connections with or without ECN have ever been seen working.
    Then uses this information to derive an obserservation about the host.
    See source for exact logic.

    :param list conditions: A list of conditions that should be merged.
        Each element should be in the input conditions of this analyzer
    :returns: the supercondition derived from the inputconditions.
        Will always be in the output conditions of this analyzer
    :rtype: list

    """


    ecn_seen_working = False
    no_ecn_seen_working = False

    ## FIRST, find out what we have seen working
    for condition in conditions:
        if condition == 'ecn.connectivity.works':
            ecn_seen_working = True
            no_ecn_seen_working = True

        elif condition == 'ecn.connectivity.broken':
            no_ecn_seen_working = True

        elif condition == 'ecn.connectivity.transient':
            ecn_seen_working = True

        elif condition == 'ecn.connectivity.offline':
            pass

    ## SECOND, determine on the actual super condition.
    # Everything is working, Yay!
    if ecn_seen_working and no_ecn_seen_working:
        super_condition = 'ecn.connectivity.super.works'

    # Nothing is working, host must me offline!
    if not ecn_seen_working and not no_ecn_seen_working:
        super_condition = 'ecn.connectivity.super.offline'

    # This hints at ECN broken. Let's verify that all observations agree:
    elif not ecn_seen_working and no_ecn_seen_working:
        if verify_all_elements_equal(conditions, 'ecn.connectivity.broken'):
            super_condition = 'ecn.connectivity.super.broken'
        else:
            super_condition = 'ecn.connectivity.super.weird'

    # This hints at ECN transient. Let's verify that all observations agree:
    elif ecn_seen_working and not no_ecn_seen_working:
        if verify_all_elements_equal(conditions, 'ecn.connectivity.transient'):
            super_condition = 'ecn.connectivity.super.transient'
        else:
            super_condition = 'ecn.connectivity.super.weird'

    return [super_condition]

def create_super_observation(db_entry):
    """
    Creates an output observation record for the db_entry

    :param dict db_entry: entry as provided by the output of the DB aggregation
    :returns: an observation formated for the databse
    :trype: dict
    """

    conditions = calculate_super_condition(db_entry['conditions'])

    dip = db_entry['_id']['dip']
    path = ['*', dip]

    value = dict()
    value['location'] = db_entry['_id']['location']

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
    observation['value'] = value

    return observation


print("--> Good morning! My name is pto-ecn-super")

ac = AnalyzerContext()
OFFSET = datetime.timedelta(hours = TIMESPAN_HOURS)
max_action_id, timespans = margin(OFFSET, ac.action_set)

# only analyze one timespan per time
time_from, time_to = timespans[0]
ac.set_result_info(max_action_id, [(time_from, time_to)])

print("--> I received {} timespans, but I am only processing one.".format(
    len(timespans)))
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
            'value.location': {'$ne': None},
            # And that are from the campaign
            'value.campaign': {'$nin': EXCLUDED_CAMPAIGNS }
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
