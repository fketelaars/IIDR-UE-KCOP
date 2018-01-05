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
import com.datamirror.ts.util.trace.Trace;
import com.google.gson.JsonObject;

/*
 * 
 */
public class KcopLiveAuditJson implements KafkaCustomOperationProcessorIF {

	private boolean includeBeforeImage;
	private String beforeImagePrefix;
	private String beforeImageSuffix;
	private HashSet<JournalControlField> journalControlFields = new HashSet<JournalControlField>();
	private String journalControlColumnPrefix;
	private String journalControlColumnSuffix;
	private boolean includeApplyTimestamp;
	private String applyTimestampColumn;
	private String kafkaTopicSuffix;

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
		loadKCOPConfigurationProperties(kafkaKcopCoordinator.getParameter(), kafkaKcopCoordinator);

	}

	/*
	 * Method that creates one or more Kafka Producer records, called for every
	 * change record
	 */
	@Override
	public ArrayList<ProducerRecord<byte[], byte[]>> createProducerRecords(KafkaKcopOperationInIF kafkaUEOperationIn,
			KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException {
		ArrayList<ProducerRecord<byte[], byte[]>> producerRecordsToReturn = new ArrayList<ProducerRecord<byte[], byte[]>>();

		String kafkaTopic = kafkaUEOperationIn.getKafkaTopicName() + kafkaTopicSuffix;
		Integer topicPartition = kafkaUEOperationIn.getPartition();
		String keyJsonRecordString = createKafkaKeyJsonRecord(kafkaUEOperationIn);
		String valueJsonRecordString = createKafkaAuditJsonRecord(kafkaUEOperationIn);

		ProducerRecord<byte[], byte[]> kafkaProducerRecord = createBinaryProducerRecord(kafkaTopic, topicPartition,
				keyJsonRecordString, valueJsonRecordString);

		// If tracing is on, issue message in the log
		if (Trace.isOn())
			Trace.trace("Producer record will be sent to Kafka topic " + kafkaTopic + ", partition " + topicPartition);

		// Add producer record to array list. In this KCOP, only 1 producer
		// record is created for every incoming change record
		producerRecordsToReturn.add(kafkaProducerRecord);

		return producerRecordsToReturn;
	}

	/*
	 * Create a JSON object with the key (this will be put in the key of the
	 * Kafka message)
	 */
	private String createKafkaKeyJsonRecord(KafkaKcopOperationInIF kafkaUEOperationIn) {
		JsonObject kafkaKeyJson = new JsonObject();

		// Add the key fields
		addFields(kafkaUEOperationIn.getKafkaAvroKeyGenericRecord(), kafkaKeyJson);

		// In case tracing is on, put retrieved value in the traces
		if (Trace.isOn())
			Trace.trace("Key JSON: " + kafkaKeyJson.toString());

		return kafkaKeyJson.toString();
	}

	/*
	 * Create a JSON object with the full audit record (this will be put in the
	 * value of the Kafka message)
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
				&& includeBeforeImage)
			addBeforeFields(kafkaUEOperationIn.getKafkaAvroBeforeValueGenericRecord(), kafkaAuditJson);

		// Add the apply timestamp if wanted
		if (includeApplyTimestamp)
			addApplyTimestamp(kafkaAuditJson);

		// In case tracing is on, put retrieved value in the traces
		if (Trace.isOn())
			Trace.trace("Value JSON: " + kafkaAuditJson.toString());

		return kafkaAuditJson.toString();
	}

	/*
	 * Append the apply timestamp
	 */
	private void addApplyTimestamp(JsonObject kafkaAuditJson) {
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000").format(new Date());
		kafkaAuditJson.addProperty(applyTimestampColumn, timeStamp);
	}

	/*
	 * Append the fields to the JSON Object
	 */
	private void addFields(GenericRecord kafkaGenericValueRecord, JsonObject kafkaAuditJson) {
		if (kafkaGenericValueRecord != null) {
			List<Field> fields = kafkaGenericValueRecord.getSchema().getFields();
			for (int i = 0; i < fields.size(); i++) {
				String name = fields.get(i).name();
				String value = kafkaGenericValueRecord.get(i).toString();
				kafkaAuditJson.addProperty(name, value);
			}
		}
	}

	/*
	 * Append the before-image fields to the JSON Object in case of update
	 */
	private void addBeforeFields(GenericRecord kafkaGenericValueRecord, JsonObject kafkaAuditJson) {
		if (kafkaGenericValueRecord != null) {
			List<Field> fields = kafkaGenericValueRecord.getSchema().getFields();
			for (int i = 0; i < fields.size(); i++) {
				String name = beforeImagePrefix + fields.get(i).name() + beforeImageSuffix;
				String value = kafkaGenericValueRecord.get(i).toString();
				kafkaAuditJson.addProperty(name, value);
			}
		}
	}

	/*
	 * This method appends the desired journal control fields to the record.
	 */
	public void addJournalControlFields(JournalHeaderIF journalHeader, JsonObject auditJson) {
		for (JournalControlField jcf : journalControlFields) {
			String jcfColumnName = journalControlColumnPrefix + jcf + journalControlColumnSuffix;
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

	/*
	 * Create a producer record with binary key and value components
	 */
	private ProducerRecord<byte[], byte[]> createBinaryProducerRecord(String kafkaTopic, Integer topicPartition,
			String keyJsonString, String valueJsonString) {
		ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;

		byte[] kafkaKeyByteArray = keyJsonString.getBytes();
		byte[] kafkaValueByteArray = valueJsonString.getBytes();

		insertKafkaAvroProducerRecord = new ProducerRecord<byte[], byte[]>(kafkaTopic, topicPartition,
				(kafkaKeyByteArray.length != 0) ? kafkaKeyByteArray : null,
				(kafkaValueByteArray.length != 0) ? kafkaValueByteArray : null);

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
	private void loadKCOPConfigurationProperties(String kcopUserExitParameter,
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
		// Show all properties in the trace
		Trace.traceAlways(kafkaKcopConfigProperties.toString());

		// Now populate the object variables
		includeBeforeImage = getPropertyBoolean(kafkaKcopConfigProperties, "includeBeforeImage", false);
		beforeImagePrefix = getProperty(kafkaKcopConfigProperties, "beforeImagePrefix", "B_");
		beforeImageSuffix = getProperty(kafkaKcopConfigProperties, "beforeImageSuffix", "");
		for (String jcc : getProperty(kafkaKcopConfigProperties, "journalControlColumns", "").split(",")) {
			// Add the journal control column to lookup list
			try {
				journalControlFields.add(JournalControlField.valueOf(jcc));
			} catch (Exception iae) {
				kafkaKcopCoordinator
						.logEvent("Error: Journal control column " + jcc + " is not valid. It will be ignored.");
			}
		}
		journalControlColumnPrefix = getProperty(kafkaKcopConfigProperties, "journalControlColumnPrefix", "AUD_");
		journalControlColumnSuffix = getProperty(kafkaKcopConfigProperties, "journalControlColumnSuffix", "");
		includeApplyTimestamp = getPropertyBoolean(kafkaKcopConfigProperties, "includeApplyTimestamp", false);
		applyTimestampColumn = getProperty(kafkaKcopConfigProperties, "applyTimestampColumn", "AUD_APPLY_TIMESTAMP");
		kafkaTopicSuffix = getProperty(kafkaKcopConfigProperties, "kafkaTopicSuffix", "-audit-json");
	}

	/*
	 * Get property string value
	 */
	private String getProperty(Properties properties, String property, String defaultValue) {
		String value = defaultValue;
		try {
			value = properties.getProperty(property);
		} catch (Exception e) {
			Trace.traceAlways("Error obtaining property " + property + ", using default value " + value);
		}
		return value;
	}

	/*
	 * Get property boolean value
	 */
	private boolean getPropertyBoolean(Properties properties, String property, boolean defaultValue) {
		boolean value = defaultValue;
		try {
			value = Boolean.parseBoolean(properties.getProperty(property));
		} catch (Exception e) {
			Trace.traceAlways(
					"Error obtaining or converting property " + property + " to boolean, using default value " + value);
		}
		return value;
	}

	/*
	 * An enum that defines all journal control fields that can be used in this
	 * user exit
	 */
	private enum JournalControlField {
		ENTTYP, TIMSTAMP, USER, JOBUSER, CCID, CNTRRN, CODE, JOBNO, JOURNAL, LIBRARY, MEMBER, OBJECT, PROGRAM, SEQNO, PARTITION, SYSTEM, UTC_TIMESTAMP,;

	}

	@Override
	public void finish(KafkaKcopReplicationCoordinatorIF arg0) throws UserExitException {
	};
}
