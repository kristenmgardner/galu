# Civis link: https://platform.civisanalytics.com/spa/#/scripts/containers/123553863
# Note: this is a copy. The container linked above runs off a script in TMC's Github.

import os
from datetime import datetime
from parsons import Redshift, Table, VAN
from requests.exceptions import HTTPError
# The line below pulls from an internal TMC repo. I've recreated what these two functions do in comments below so Galvanize folks can use them in other projects.
from canalespy import logger, setup_environment

# logger definition:

# import logging
# logger = logging.getLogger(__name__)
# _handler = logging.StreamHandler()
# _formatter = logging.Formatter('%(levelname)s %(message)s')
# _handler.setFormatter(_formatter)
# logger.addHandler(_handler)
# logger.setLevel('INFO')

# setup_environment definition:

# os.environ['REDSHIFT_DB'] = os.environ['REDSHIFT_DATABASE']
# os.environ['REDSHIFT_USERNAME'] = os.environ['REDSHIFT_CREDENTIAL_USERNAME'] 
# os.environ['REDSHIFT_PASSWORD'] = os.environ['REDSHIFT_CREDENTIAL_PASSWORD'] 
# os.environ['S3_TEMP_BUCKET'] = 'parsons-tmc'

api_key = os.environ['VAN_PASSWORD']
relationship_table = os.environ['RELATIONSHIP_TABLE']
log_schema = os.environ['LOG_SCHEMA']

success_table = log_schema + '.ea_relationship_successes'
error_table = log_schema + '.ea_relationship_errors'

setup_environment()
van = VAN(api_key=api_key, db='EveryAction')
rs = Redshift()

def main():
  
  # This queries a table of relationship data that has been loaded into Redshift
  r = rs.query(f"""select primary_contact_vanid
                   , secondary_contact_vanid
                   , relationship_type_id
                   from {relationship_table}
                   where primary_contact_vanid is not null
                   and secondary_contact_vanid is not null
                   and relationship_type_id is not null
                   and relationship_type_id != '#N/A';""") # This line is here because the member uses a VLOOKUP to assign IDs
  
  logger.info(f"Found {r.num_rows} relationships to apply.")
  
  if r.num_rows > 0:
  
    # Here we set up two empty lists which we'll populate in the for loop below. One will contain records of successful applications of relationships, the other errors.
    successes = []
    errors = []
    applied_at = str(datetime.now()).split(".")[0]

    for row in r:
      primary_vanid = int(row['primary_contact_vanid'])
      secondary_vanid = int(row['secondary_contact_vanid'])
      relationship_id = int(row['relationship_type_id'])

      # create_relationship returns None so we can't check a response for logging
      try:
          van.create_relationship(primary_vanid, secondary_vanid, relationship_id)
          successes.append({
            "primary_vanid": primary_vanid,
            "secondary_vanid": secondary_vanid,
            "relationship_id": relationship_id,
            "applied_at": applied_at
          })

      except HTTPError as e:
          logger.info(f"There was an issue creating relationship between {primary_vanid} and {secondary_vanid}. Error: {e}")
          errors.append({
            "primary_vanid": primary_vanid,
            "secondary_vanid": secondary_vanid,
            "relationship_id": relationship_id,
            "errored_at": applied_at,
            "error": str(e)[:999]
          })

    logger.info(f"{len(successes)} relationships created successfully, {len(errors)} errors.")

    if len(successes) > 0:
      s = Table(successes)
      rs.copy(s, success_table, if_exists='append', alter_table=True)

    if len(errors) > 0:
      er = Table(errors)
      rs.copy(er, error_table, if_exists='append', alter_table=True)
    
if __name__ == '__main__':
    main()
