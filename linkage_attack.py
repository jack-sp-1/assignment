# Code for PII transform in AWS glue
# Script generated for node Detect PII
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    AmazonS3_node166,
    [
        "PERSON_NAME",
        "EMAIL",
        "CREDIT_CARD",
        "IP_ADDRESS",
        "MAC_ADDRESS",
        "PHONE_NUMBER",
        "USA_PASSPORT_NUMBER",
        "USA_SSN",
        "USA_ITIN",
        "BANK_ACCOUNT",
        "USA_DRIVING_LICENSE",
        "USA_HCPCS_CODE",
        "USA_NATIONAL_DRUG_CODE",
        "USA_NATIONAL_PROVIDER_IDENTIFIER",
        "USA_DEA_NUMBER",
        "USA_HEALTH_INSURANCE_CLAIM_NUMBER",
        "USA_MEDICARE_BENEFICIARY_IDENTIFIER",
    ],
    0.3,
    0.2,
)


def maskDf(df, keys):
    if not keys:
        return df
    df_to_mask = df.toDF()
    for key in keys:
        df_to_mask = df_to_mask.withColumn(key, lit("**************"))
    return DynamicFrame.fromDF(df_to_mask, glueContext, "updated_masked_df")


DetectPII_node166 = maskDf(
   
