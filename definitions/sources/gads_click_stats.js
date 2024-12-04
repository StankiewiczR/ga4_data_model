if (constants.GADS_GET_DATA) {
    declare({
        type: "declaration",
        schema: constants.GADS_SOURCE_DATASET,
        ...(constants.GADS_SOURCE_PROJECT) && {
            database: constants.GADS_SOURCE_PROJECT
        },
        name: "ads_ClickStats_" + constants.GADS_CUSTOMER_ID
    });
}