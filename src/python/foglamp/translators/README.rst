OMF Translator
==============

Starting the OMF Translator
---------------------------

- consider the constant _TRACE_EXECUTION, false for default : Enable/Disable info messages

- set the staring point using :
    - identify the correct id, using for example :
        SELECT MAX(ID) FROM foglamp.readings;

    - update last_object properly :
        - UPDATE foglamp.streams SET last_object=1, ts=now() WHERE id=1;

- it could be executed as is without parameters :
    python -m foglamp.translators.omf_translator

Note
----
- the configuration table should be update to change the producerToken, using for example, the row will be recreated :
    - DELETE FROM foglamp.configuration WHERE key='OMF_TRANS ';

- foglamp_init_data.sql will load :
    INSERT INTO foglamp.destinations(id,description, ts) VALUES (1,'OMF', now());
    INSERT INTO foglamp.streams(id,destination_id,description, last_object,ts) VALUES (1,1,'OMF', 1,now());

- block_size identifies the number of rows to send for each execution
- it uses foglamp.streams to track the information to send

- Temporary/Useful SQL code used for dev:

    SELECT MAX(ID) FROM foglamp.readings;

    UPDATE foglamp.streams SET last_object=1, ts=now() WHERE id=1;

    SELECT * FROM foglamp.streams;

    SELECT * FROM foglamp.readings WHERE id > 98021 ORDER by USER_ts;

    SELECT * FROM foglamp.readings WHERE id >= 98021 and reading ? 'lux' ORDER by USER_ts;

