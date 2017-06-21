package com.mastercard.pclo.redemption.file.generation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buxwatch.pojo.offer.RedemptionMode;
import com.google.common.collect.ImmutableMap;
import com.mastercard.pclo.file.generation.FileGenerator;
import com.mastercard.pclo.file.generation.offer.ConvertionUtility;
import com.mastercard.pclo.file.generation.offer.FileFieldDefinition;
import com.mastercard.pclo.file.generation.redemption.RedemptionFileData;
import com.mastercard.pclo.file.generation.redemption.RedemptionRecordComparator;
import com.mastercard.pclo.offer.file.context.FileContext;
import com.mastercard.pclo.util.RedemptionFileBulkIdType;

import edu.emory.mathcs.backport.java.util.Collections;

public class DefaultRedemptionFileGenerator extends FileGenerator {

	private static final Logger log = LoggerFactory.getLogger(DefaultRedemptionFileGenerator.class);

	private List<FileFieldDefinition> inputFieldDefinition;

	private String inputSeparator;

	private List<FileFieldDefinition> allRedemptionFieldDefinition;

	private Map<String, List<FileFieldDefinition>> outputFieldDefinition;

	private Map<String, List<FileFieldDefinition>> headerFieldDefinition;

	private Map<String, List<FileFieldDefinition>> trailerFieldDefinition;

	private static final Map<Integer, String> POSITIVE_VALUE_MAP = ImmutableMap.<Integer, String>builder()
			.put(0, "{").put(1, "A").put(2, "B").put(3, "C").put(4, "D").put(5, "E").put(6, "F").put(7, "G").put(8, "H").put(9, "I").build();

	private static final Map<Integer, String> NEGATIVE_VALUE_MAP = ImmutableMap.<Integer, String>builder()
			.put(0, "}").put(1, "J").put(2, "K").put(3, "L").put(4, "M").put(5, "N").put(6, "O").put(7, "P").put(8, "Q").put(9, "R").build();

	@Override
	public void processRecords(FileContext context) {
		Map<String, Collection<String>> processedOutputRecords = new HashMap<String, Collection<String>>();
		Collection<String> proccessedRedemptionRecordList = new ArrayList<String>();
		Collection<String> proccessedAirMilesRecordList = new ArrayList<String>();
		Map<String, String> headerMap = new HashMap<String, String>();
		
		RedemptionFileGenerationContext fileContext = (RedemptionFileGenerationContext) context.getFileContext();
		try {
			Date date = new Date();
			context.setFieldDefinition(
					headerFieldDefinition.get(fileContext.getRedemptionFileConfiguration().getVersion().get(0)));
			context.setValue(processRecordHeader(fileContext, date, fileContext.getRedemptionFileConfiguration().getVersion().get(0)));
			headerMap.put(fileContext.getRedemptionFileConfiguration().getVersion().get(0), objectToRecord(context));
			fileContext.setHeader(headerMap);

			fileContext.setProcessedRedemptions(duplicateRedemptionChecker(context));

			context.setFieldDefinition(
					outputFieldDefinition.get(fileContext.getRedemptionFileConfiguration().getVersion().get(0)));
			context.setSeparator(null);
			for (RedemptionFileData redemptionRecord : fileContext.getProcessedRedemptions()) {
				context.setValue(redemptionRecord);
				proccessedRedemptionRecordList.add(objectToRecord(context));
			}
			processedOutputRecords.put(fileContext.getRedemptionFileConfiguration().getVersion().get(0), proccessedRedemptionRecordList);
			fileContext.setOutputRecordList(processedOutputRecords);

			context.setFieldDefinition(
					trailerFieldDefinition.get(fileContext.getRedemptionFileConfiguration().getVersion().get(0)));
			context.setValue(processRecordTrailer(fileContext, date));
			fileContext.setTrailer(objectToRecord(context));

			if (fileContext.getRedemptionFileConfiguration().getVersion().size() > 1 && 
					fileContext.getRedemptionFileConfiguration().getVersion().contains(RedemptionFileBulkIdType.l1.getName())) {
				Collection<RedemptionFileData> amRedemptionsList = fileContext.getAmRedemptionList();
				context.setFieldDefinition(outputFieldDefinition.get(fileContext.getRedemptionFileConfiguration().getVersion().get(1)));
				for (RedemptionFileData redemptionRecord : amRedemptionsList) {
					context.setValue(redemptionRecord);
					proccessedAirMilesRecordList.add(objectToRecord(context));
				}
				processedOutputRecords.put(fileContext.getRedemptionFileConfiguration().getVersion().get(1), proccessedAirMilesRecordList);
				fileContext.setOutputRecordList(processedOutputRecords);
				
				context.setFieldDefinition(headerFieldDefinition.get(fileContext.getRedemptionFileConfiguration().getVersion().get(1)));
				context.setValue(processRecordHeader(fileContext, date, fileContext.getRedemptionFileConfiguration().getVersion().get(1)));
				headerMap.put(fileContext.getRedemptionFileConfiguration().getVersion().get(1), objectToRecord(context));
				fileContext.setHeader(headerMap);
			}

			Map<String, String> messages = contructMessages(fileContext);
			fileContext.setMessages(messages);
			context.setFileContext(fileContext);
		} catch (Exception exception) {
			log.error("Error in processing input records", exception);
			throw new RuntimeException("Error in processing input records", exception);
		}
	}

	private Collection<RedemptionFileData> duplicateRedemptionChecker(FileContext context) {
		RedemptionFileGenerationContext redemptionContext = (RedemptionFileGenerationContext) context.getFileContext();
		List<RedemptionFileData> redemptionRecordsList = new ArrayList<RedemptionFileData>();
		List<RedemptionFileData> localDuplicateRedemptionRecordsList = new ArrayList<RedemptionFileData>();
		List<String> redemptionConfirmationRecordList = new ArrayList<String>();
		List<RedemptionFileData> amRedemptionRecordsList = new ArrayList<RedemptionFileData>();

		int totalPoints = 0, totalMilesIssued = 0;
		try {
			context.setFieldDefinition(inputFieldDefinition);
			context.setSeparator(inputSeparator);

			for (String record : redemptionContext.getInputRecordList()) {
				RedemptionFileData redemptionData = new RedemptionFileData();
				context.setValue(redemptionData);
				recordToObject(context, record);
				redemptionData.init(redemptionContext.getRedemptionFileConfiguration());
				
				String version = redemptionContext.getRedemptionFileConfiguration().getVersion().get(0);
				if ( version.equals(RedemptionFileBulkIdType.rm37v1.getName()) 
						|| version.equals(RedemptionFileBulkIdType.rm37v2.getName()) 
						|| version.equals(RedemptionFileBulkIdType.tvh0v2.getName())) {
					redemptionData.setBankProductCode(redemptionContext.getRedemptionFileConfiguration().getBankProductCode());
				}

				int localSearchResult = Collections.binarySearch(redemptionRecordsList, redemptionData,
						new RedemptionRecordComparator());
				if (localSearchResult >= 0) {
					localDuplicateRedemptionRecordsList.add(redemptionData);
				} else {
					redemptionConfirmationRecordList.add(redemptionData.getActivationId() + ","
							+ redemptionData.getRedemptionId() + "," + redemptionData.getTargeted());

					if (redemptionData.getRedemptionMode().equals(String.valueOf(RedemptionMode.POINTS.ordinal()))) {
						if (redemptionContext.getRedemptionFileConfiguration().getVersion().size() > 1 && 
								redemptionContext.getRedemptionFileConfiguration().getVersion().contains(RedemptionFileBulkIdType.l1.getName())) {
							Float miles = (float) Math.round(ConvertionUtility.getFloat(redemptionData.getCashBack())
									/ ConvertionUtility.getFloat(redemptionData.getEarnRate()));
							totalMilesIssued += miles.intValue();
							redemptionData.setMilesIssued(getSignedValue(miles.intValue()));
							redemptionData.setDollarSpend(getSignedValue(ConvertionUtility.getFloat(redemptionData.getTransactionAmount()).intValue()));
							amRedemptionRecordsList.add(redemptionData);
						} else {
							Float adjVal = ConvertionUtility.getFloat(redemptionData.getPointsEarned()) * 100;
							redemptionData.setAdjustmentValue(String.valueOf(adjVal.intValue()));
							totalPoints += Integer.valueOf(redemptionData.getAdjustmentValue());
						}
					}
					redemptionRecordsList.add(redemptionData);
				}
			}

			if (redemptionContext.getRedemptionFileConfiguration().getVersion().size() > 1 && 
					redemptionContext.getRedemptionFileConfiguration().getVersion().contains(RedemptionFileBulkIdType.l1.getName())) {
				redemptionContext.setAmSequenceUpdateData("sequenceNumber,"	+ redemptionContext.getRedemptionFileConfiguration().getSource()
						+ "," + redemptionContext.getRedemptionFileConfiguration().getRedemptionFileBulkId().get(1)
						+ "," + String.valueOf(Integer.valueOf(redemptionContext.getAmSequence()) + 1) 
						+ "," + DateFormatUtils.format(new Date(), "yyyy-M-dd hh:mm:ss"));
			}
			redemptionContext.setAmRedemptionList(amRedemptionRecordsList);
			redemptionContext.setLocalDuplicateRedemptionList(localDuplicateRedemptionRecordsList);
			redemptionContext.setRedemptionConfirmationList(redemptionConfirmationRecordList);
			redemptionContext.setTotalPoints(totalPoints);
			redemptionContext.setTotalMilesIssued(getSignedValue(totalMilesIssued));
		} catch (Exception exception) {
			log.error("Error in converting record to object", exception);
			throw new RuntimeException("Error in converting record to object", exception);
		}
		return redemptionRecordsList;
	}

	private RedemptionFileData processRecordHeader(RedemptionFileGenerationContext context, Date date, String version) {
		RedemptionFileData recordHeader = new RedemptionFileData();
		Map<String, Collection<String>> outputRecordMap = context.getOutputRecordMap();
		try {
			recordHeader
					.setRecordDate(ConvertionUtility.getDate(String.valueOf(date.getTime() / 1000), "yyyyMMdd", true));
			recordHeader
					.setRecordTime(ConvertionUtility.getDate(String.valueOf(date.getTime() / 1000), "HHmmss", true));
			if (!version.equals(RedemptionFileBulkIdType.tvh0v1.getName())) {
				recordHeader.setHeaderMemberIca(context.getRedemptionFileConfiguration().getMemberIcaCode());
			}
			recordHeader.setHeaderFileName(context.getRedemptionFileConfiguration().getSource() + "_adj_"
					+ ConvertionUtility.getDate(String.valueOf(date.getTime() / 1000), "yyMMdd", true) + ".txt");
			recordHeader
					.setAmRecordCount((outputRecordMap == null || outputRecordMap
							.isEmpty()) ? String.valueOf(0) : String.valueOf(outputRecordMap.get(RedemptionFileBulkIdType.l1.getName()).size()));
			recordHeader
					.setAmSequenceNumber(context.getAmSequence() == null ? String
							.valueOf(1) : String.valueOf(Integer.valueOf(context.getAmSequence()) + 1));
			recordHeader.setProviderCode(context.getRedemptionFileConfiguration().getProviderCode());
			recordHeader.setTotalMilesIssued(context.getTotalMilesIssued());
			recordHeader.setHeaderFiller(StringUtils.EMPTY);
		} catch (Exception exception) {
			log.error("Error in converting record to object", exception);
			throw new RuntimeException("Error in converting record to object", exception);
		}
		return recordHeader;
	}

	private RedemptionFileData processRecordTrailer(RedemptionFileGenerationContext context, Date date) {
		RedemptionFileData recordTrailer = new RedemptionFileData();
		Map<String, Collection<String>> outputRecordMap = context.getOutputRecordMap();
		try {
			recordTrailer
					.setRecordDate(ConvertionUtility.getDate(String.valueOf(date.getTime() / 1000), "yyyyMMdd", true));
			recordTrailer
					.setRecordTime(ConvertionUtility.getDate(String.valueOf(date.getTime() / 1000), "hhmmss", true));
			recordTrailer.setTrailerMemberIca(context.getRedemptionFileConfiguration().getMemberIcaCode());
			recordTrailer.setTrailerFiller(StringUtils.EMPTY);
			recordTrailer.setTrailerRecordCount(outputRecordMap.get(context.getRedemptionFileConfiguration().getVersion().get(0)).isEmpty() ? String.valueOf(0)
					: String.valueOf(outputRecordMap.get(context.getRedemptionFileConfiguration().getVersion().get(0)).size()));
			recordTrailer.setTotalRecordCount(outputRecordMap.get(context.getRedemptionFileConfiguration().getVersion().get(0)).isEmpty() ? String.valueOf(0)
					: String.valueOf(outputRecordMap.get(context.getRedemptionFileConfiguration().getVersion().get(0)).size() + 2));
			recordTrailer.setTrailerFileName(context.getRedemptionFileConfiguration().getSource() + "_adj_"
					+ ConvertionUtility.getDate(String.valueOf(date.getTime() / 1000), "yyMMdd", true) + ".txt");
			recordTrailer.setTrailerTotalPoints(String.valueOf(context.getTotalPoints()));
		} catch (Exception exception) {
			log.error("Error in converting record to object", exception);
			throw new RuntimeException("Error in converting record to object", exception);
		}
		return recordTrailer;
	}

	private Map<String, String> contructMessages(RedemptionFileGenerationContext context) {
		int totalRedemptionsCount = 0, localDuplicateRedemptionCount = 0;
		Map<String, String> messages = new HashMap<String, String>();
		String fileType = null;
		try {
			totalRedemptionsCount = context.getOutputRecordMap().get(context.getRedemptionFileConfiguration().getVersion()
							.get(0)) != null ? context.getOutputRecordMap().get(context.getRedemptionFileConfiguration().getVersion().get(0)).size() : 0;
			if (context.getRedemptionFileConfiguration().getVersion().get(0).equals(RedemptionFileBulkIdType.re89.getName())) {
				fileType = "adjustment";
			} else {
				fileType = "rebate";
			}
			localDuplicateRedemptionCount = context.getLocalDuplicateRedemptionList() != null
					? context.getLocalDuplicateRedemptionList().size() : 0;
			StringBuilder builder = new StringBuilder();
			builder.append("Total " + fileType + " record count: " + totalRedemptionsCount)
					.append(System.lineSeparator());
			if (localDuplicateRedemptionCount > 0) {
				builder.append(
						"No. of duplicate " + fileType + " record found (local/file): " + localDuplicateRedemptionCount)
						.append(System.lineSeparator());
			}
			messages.put("redemption_file_gen_message", builder.toString());
		} catch (Exception exception) {
			log.error("Error in generating redemption messages", exception);
		}
		return messages;
	}

	private String getSignedValue(int miles) {
		String formattedValue = null;
		try {
			char[] s = String.valueOf(miles).toCharArray();
			s[s.length - 1] = (miles > 0) ? POSITIVE_VALUE_MAP.get(Character.getNumericValue(s[s.length - 1])).toCharArray()[0]
					: NEGATIVE_VALUE_MAP.get(Character.getNumericValue(s[s.length - 1])).toCharArray()[0];
			formattedValue = String.valueOf(s);
		} catch (Exception exception) {
			log.error("Error in getting signed value", exception);
		}
		return formattedValue;
	}

	public Map<String, List<FileFieldDefinition>> getOutputFieldDefinition() {
		return outputFieldDefinition;
	}

	public void setOutputFieldDefinition(Map<String, List<FileFieldDefinition>> outputFieldDefinition) {
		this.outputFieldDefinition = outputFieldDefinition;
	}

	public Map<String, List<FileFieldDefinition>> getHeaderFieldDefinition() {
		return headerFieldDefinition;
	}

	public void setHeaderFieldDefinition(Map<String, List<FileFieldDefinition>> headerFieldDefinition) {
		this.headerFieldDefinition = headerFieldDefinition;
	}

	public Map<String, List<FileFieldDefinition>> getTrailerFieldDefinition() {
		return trailerFieldDefinition;
	}

	public void setTrailerFieldDefinition(Map<String, List<FileFieldDefinition>> trailerFieldDefinition) {
		this.trailerFieldDefinition = trailerFieldDefinition;
	}

	public List<FileFieldDefinition> getInputFieldDefinition() {
		return inputFieldDefinition;
	}

	public void setInputFieldDefinition(List<FileFieldDefinition> inputFieldDefinition) {
		this.inputFieldDefinition = inputFieldDefinition;
	}

	public List<FileFieldDefinition> getAllRedemptionFieldDefinition() {
		return allRedemptionFieldDefinition;
	}

	public void setAllRedemptionFieldDefinition(List<FileFieldDefinition> allRedemptionFieldDefinition) {
		this.allRedemptionFieldDefinition = allRedemptionFieldDefinition;
	}

	public String getInputSeparator() {
		return inputSeparator;
	}

	public void setInputSeparator(String inputSeparator) {
		this.inputSeparator = inputSeparator;
	}

}
