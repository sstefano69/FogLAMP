OMF translator
==============

Introduction
------------
The OMF translator is a plugin output formatter for the FogLAMP appliance. It is loaded by the send process (see The FogLAMP Sending Processs) and runs in the context of the send process, to send the reading data to a PI Server (or Connector) using the OSIsoft OMF format. The abstract sequence of operations in the process to send readings via OMF to a PI System is shown below.


Note
------------

Temporary SQL code used for dev :

    INSERT INTO foglamp.destinations (id,description, ts ) VALUES (1,'OMF', now() );

    INSERT INTO foglamp.streams (id,destination_id,description, reading_id,ts ) VALUES (1,1,'OMF', 666,now());

    SELECT * FROM foglamp.streams;

    SELECT * FROM foglamp.destinations;

    UPDATE foglamp.streams SET reading_id=4194 WHERE id=1;


Tests execution
-------------

.. code-block:: bash

    foglamp$ TBD
