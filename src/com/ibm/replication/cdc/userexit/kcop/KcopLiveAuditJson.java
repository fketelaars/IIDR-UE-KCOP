package com.ibm.replication.cdc.userexit.kcop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.datamirror.ts.target.publication.userexit.JournalHeaderIF;
import com.datamirror.ts.target.publication.userexit.ReplicationEventTypes;
import com.datamirror.ts.target.publication.userexit.UserExitException;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaCustomOperationProcessorIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopOperationInIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopReplicationCoordinatorIF;
import com.google.gson.JsonObject;

/*
 * 
 */
public class KcopLiveAuditJson implements KafkaCustomOperationProcessorIF {

	private Properties kcopConfigurationProperties;
	private HashSet<JournalControlField> journalControlFields = new HashSet<JournalControlField>();

	/*
	 * Initialize the KCOP - set trigger events and load properties
	 */
	@Override
	public void init(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException {
		kafkaKcopCoordinator.logEvent("Initializing KCOP user exit " + this.getClass().getName());
		// Subscribe to all operations
		kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_INSERT_EVENT);
		kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_DELETE_EVENT);
		kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_UPDATE_EVENT);

		// Load the properties
		kcopConfigurationProperties = loadKCOPConfigurationProperties(kafkaKcopCoordinator.getParameter(),
				kafkaKcopCoordinator);

		// Fill the set of journal control fields (reused many times)
		for (String jcc : kcopConfigurationProperties.getProperty("journalControlColumns").split(",")) {
			journalControlFields.add(JournalControlField.valueOf(jcc));
		}

	}

	@Override
	public ArrayList<ProducerRecord<byte[], byte[]>> createProducerRecords(KafkaKcopOperationInIF kafkaUEOperationIn,
			KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException {
		ArrayList<ProducerRecord<byte[], byte[]>> producerRecordsToReturn = new ArrayList<ProducerRecord<byte[], byte[]>>();

		String kafkaValueAuditRecord = createKafkaAuditJsonRecord(kafkaUEOperationIn);

		ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;

		insertKafkaAvroProducerRecord = createKafkaAuditBinaryProducerRecord(kafkaValueAuditRecord, kafkaUEOperationIn);

		producerRecordsToReturn.add(insertKafkaAvroProducerRecord);

		return producerRecordsToReturn;
	}

	/*
	 * Create a JSON object with the full audit record
	 */
	private String createKafkaAuditJsonRecord(KafkaKcopOperationInIF kafkaUEOperationIn) {
		JsonObject kafkaAuditJson = new JsonObject();

		// Add the journal control fields
		addJournalControlFields(kafkaUEOperationIn.getUserExitJournalHeader(), kafkaAuditJson);

		// Add the after image in case of insert and update, before image in
		// case of delete
		if (kafkaUEOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_INSERT_EVENT
				|| kafkaUEOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_UPDATE_EVENT) {
			addFields(kafkaUEOperationIn.getKafkaAvroValueGenericRecord(), kafkaAuditJson);
		} else {
			addFields(kafkaUEOperationIn.getKafkaAvroBeforeValueGenericRecord(), kafkaAuditJson);
		}

		// In case of update, maybe add the before-image columns
		if (kafkaUEOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_UPDATE_EVENT
				&& Boolean.parseBoolean(kcopConfigurationProperties.getProperty("includeBeforeImage", "false")))
			addBeforeFields(kafkaUEOperationIn.getKafkaAvroBeforeValueGenericRecord(), kafkaAuditJson);

		// Add the apply timestamp if wanted
		if (Boolean.parseBoolean(kcopConfigurationProperties.getProperty("includeApplyTimestamp", "true")))
			addApplyTimestamp(kafkaAuditJson);

		return kafkaAuditJson.toString();

	}

	/*
	 * Append the apply timestamp
	 */
	private void addApplyTimestamp(JsonObject kafkaAuditJson) {
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000").format(new Date());
		kafkaAuditJson.addProperty(kcopConfigurationProperties.getProperty("applyTimestampColumn"), timeStamp);
	}

	/*
	 * Append the fields to the JSON Object
	 */
	private void addFields(GenericRecord kafkaGenericAvroValueRecord, JsonObject kafkaAuditJson) {
		if (kafkaGenericAvroValueRecord != null) {
			List<Field> fields = kafkaGenericAvroValueRecord.getSchema().getFields();
			for (int i = 0; i < fields.size(); i++) {
				String name = fields.get(i).name();
				String value = kafkaGenericAvroValueRecord.get(i).toString();
				kafkaAuditJson.addProperty(name, value);
			}
		}
	}

	/*
	 * Append the before-image fields to the JSON Object in case of update
	 */
	private void addBeforeFields(GenericRecord kafkaGenericAvroValueRecord, JsonObject kafkaAuditJson) {
		if (kafkaGenericAvroValueRecord != null) {
			List<Field> fields = kafkaGenericAvroValueRecord.getSchema().getFields();
			for (int i = 0; i < fields.size(); i++) {
				String name = kcopConfigurationProperties.getProperty("beforeImagePrefix") + fields.get(i).name()
						+ kcopConfigurationProperties.getProperty("beforeImageSuffix");
				String value = kafkaGenericAvroValueRecord.get(i).toString();
				kafkaAuditJson.addProperty(name, value);
			}
		}
	}

	/*
	 * This method appends the desired journal control fields to the record.
	 */
	public void addJournalControlFields(JournalHeaderIF journalHeader, JsonObject auditJson) {
		for (JournalControlField jcf : journalControlFields) {
			String jcfColumnName = kcopConfigurationProperties.getProperty("journalControlColumnPrefix") + jcf
					+ kcopConfigurationProperties.getProperty("journalControlColumnSuffix");
			if (jcf != JournalControlField.ENTTYP)
				auditJson.addProperty(jcfColumnName, getJournalControlField(journalHeader, jcf));
			else
				auditJson.addProperty(jcfColumnName, translateEnttyp(getJournalControlField(journalHeader, jcf)));
		}
	}

	/*
	 * Get the value from the journal control column
	 */
	private String getJournalControlField(JournalHeaderIF journalHeader, JournalControlField journalControlField) {
		switch (journalControlField) {
		case ENTTYP:
			return journalHeader.getEntryType();
		case JOBUSER:
			return journalHeader.getJobUser();
		case TIMSTAMP:
			return journalHeader.getTimestamp();
		case USER:
			return journalHeader.getUserName();
		case CCID:
			return journalHeader.getCommitID();
		case CNTRRN:
			return journalHeader.getRelativeRecNum();
		case CODE:
			return journalHeader.getJournalCode();
		case JOBNO:
			return journalHeader.getJobNumber();
		case LIBRARY:
			return journalHeader.getLibrary();
		case MEMBER:
			return journalHeader.getMemberName();
		case OBJECT:
			return journalHeader.getObjectName();
		case PROGRAM:
			return journalHeader.getProgramName();
		case SEQNO:
			return journalHeader.getSeqNo();
		case SYSTEM:
			return journalHeader.getSystemName();
		case UTC_TIMESTAMP:
			return journalHeader.getUtcTimestamp();
		case PARTITION:
		default:
			return "";
		}
	}

	private ProducerRecord<byte[], byte[]> createKafkaAuditBinaryProducerRecord(String liveAuditRecord,
			KafkaKcopOperationInIF kafkaUEOperationIn) {
		ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;

		byte[] kafkaAvroValueByteArray = liveAuditRecord.getBytes();

		insertKafkaAvroProducerRecord = new ProducerRecord<byte[], byte[]>(
				kafkaUEOperationIn.getKafkaTopicName() + kcopConfigurationProperties.getProperty("kafkaTopicSuffix"),
				kafkaUEOperationIn.getPartition(), null,
				(kafkaAvroValueByteArray.length != 0) ? kafkaAvroValueByteArray : null);

		return insertKafkaAvroProducerRecord;

	}

	/*
	 * Translate the entry type to a unified string across platforms
	 */
	private String translateEnttyp(String enttyp) {
		String convertedEvent;
		if ("PT".equals(enttyp) || "PX".equals(enttyp) || "RR".equals(enttyp)) {
			convertedEvent = "I";
		} else if ("UP".equals(enttyp)) {
			convertedEvent = "U";
		} else if ("DL".equals(enttyp)) {
			convertedEvent = "D";
		} else {
			convertedEvent = "R";
		}

		return convertedEvent;
	}

	// Load the properties from the specified properties file, or use the
	// defaults
	private Properties loadKCOPConfigurationProperties(String kcopUserExitParameter,
			KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException {
		InputStream configFileStream;
		Properties kafkaKcopConfigProperties = new Properties();

		// Load the configuration properties from the specified input file,
		// otherwise the KcopLiveAuditJson.properties file
		String propertiesFile = this.getClass().getSimpleName() + ".properties";
		if (!kcopUserExitParameter.isEmpty())
			propertiesFile = kcopUserExitParameter;
		// Try loading the properties
		try {
			URL fileURL = this.getClass().getClassLoader().getResource(propertiesFile);
			kafkaKcopCoordinator.logEvent(
					"Loading properties for user exit " + this.getClass().getName() + " from file " + fileURL);
			configFileStream = this.getClass().getClassLoader().getResourceAsStream(propertiesFile);
			kafkaKcopConfigProperties.load(configFileStream);
			configFileStream.close();
		} catch (FileNotFoundException e) {
			kafkaKcopCoordinator
					.logEvent("Properties file " + propertiesFile + " was not found. Default settings will be used.");
		} catch (IOException e) {
			kafkaKcopCoordinator.logEvent(
					"An IOException was encountered when attempting to load the properties file provided by the user.");
			throw new UserExitException(e.getMessage());
		}
		return kafkaKcopConfigProperties;
	}

	/*
	 * An enum that defines all journal control fields that can be used in this
	 * user exit
	 */
	public enum JournalControlField {
		ENTTYP, TIMSTAMP, USER, JOBUSER, CCID, CNTRRN, CODE, JOBNO, JOURNAL, LIBRARY, MEMBER, OBJECT, PROGRAM, SEQNO, PARTITION, SYSTEM, UTC_TIMESTAMP,;

	}

	@Override
	public void finish(KafkaKcopReplicationCoordinatorIF arg0) throws UserExitException {
	};
}
