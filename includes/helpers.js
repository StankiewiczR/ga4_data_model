function unnest_column(column_to_unnest, key_to_extract, value_type="string_value") {
    return `(select value.${value_type} from unnest(${column_to_unnest}) where key = '${key_to_extract}')`;
};

function extract_url(input_string) {
    return `regexp_extract(${input_string}, '(?:http[s]?://)?(?:www\\\\.)?(.*?)(?:(?:/|:)(?:.)*|$)')`;
};

function group_channels(source, medium, source_category) {
    return `case
                when
                    (
                    ${source} is null
                        and ${medium} is null
                    )
                    or (
                    ${source} = '(direct)'
                    and (${medium} = '(none)' or ${medium} = '(not set)')
                    )
                    then 'Direct'
                when
                    ${source_category} = 'SOURCE_CATEGORY_SHOPPING'
                    and regexp_contains(${medium},r"^(.*cp.*|ppc|retargeting|paid.*)$")
                    then 'Paid Shopping'
                when
                    ${source_category} = 'SOURCE_CATEGORY_SEARCH'
                    and regexp_contains(${medium}, r"^(.*cp.*|ppc|retargeting|paid.*)$")
                    then 'Paid Search'
                when
                    (
                    regexp_contains(${source}, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)")
                    or ${source_category} = 'SOURCE_CATEGORY_SOCIAL'
                    )
                    and regexp_contains(${medium}, r"^(.*cp.*|ppc|retargeting|paid.*)$")
                    then 'Paid Social'
                when
                    (${source_category} = 'SOURCE_CATEGORY_VIDEO' AND regexp_contains(${medium},r"^(.*cp.*|ppc|retargeting|paid.*)$"))
                    or ${source} = 'dv360_video'
                    then 'Paid Video'
                when
                    regexp_contains(${medium}, r"^(display|cpm|banner|expandable|interstitial)$")
                    or ${source} = 'dv360_display'
                    then 'Display'
                when
                    regexp_contains(${medium}, r"^(.*cp.*|ppc|retargeting|paid.*)$")
                    then 'Paid Other'
                when
                    ${source_category} = 'SOURCE_CATEGORY_SHOPPING'
                    then 'Organic Shopping'
                when
                    regexp_contains(${source}, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)")
                    or ${medium} in ("social","social-network","social-media","sm","social network","social media")
                    or ${source_category} = 'SOURCE_CATEGORY_SOCIAL'
                    then 'Organic Social'
                when
                    ${source_category} = 'SOURCE_CATEGORY_VIDEO'
                    or regexp_contains(${medium}, r"^(.*video.*)$")
                    then 'Organic Video'
                when
                    ${medium} = 'organic'
                    or ${source_category} = 'SOURCE_CATEGORY_SEARCH'
                    then 'Organic Search'
                when
                    ${medium} in ("referral", "app", "link")
                    then 'Referral'
                when
                    regexp_contains(${medium}, r"email|e-mail|e_mail|e mail")
                    or regexp_contains(${source}, r"email|e-mail|e_mail|e mail")
                    then 'Email'
                when
                    regexp_contains(${medium}, r"affiliate|affiliates")
                    then 'Affiliates'
                when
                    ${medium} = 'audio'
                    then 'Audio'
                when
                    ${medium} = 'sms'
                    or ${source} = 'sms'
                    then 'SMS'
                when
                    regexp_contains(${medium}, r"(mobile|notification|push$)") or ${source} = 'firebase'
                    then 'Push Notifications'
                else '(Other)'
                end`
}

function updatePaidSearchTrafficSource(struct, structColumn, tableAlias, gadsCampaignName) {
    let trafficSource = {
        cpcValue: '',
        cpmValue: ''
    };
    let userTrafficSourceColumn = '';
    if (structColumn === 'manual_source') {
        trafficSource.cpcValue = 'google';
        trafficSource.cpmValue = 'dbm';
        userTrafficSourceColumn = 'source';
    } else if (structColumn === 'manual_medium') {
        trafficSource.cpcValue = 'cpc';
        trafficSource.cpmValue = 'cpm';
        userTrafficSourceColumn = 'medium';
    } else if (structColumn === 'manual_campaign_name') {
        trafficSource.cpcValue = '(cpc)';
        trafficSource.cpmValue = '(cpm)';
        userTrafficSourceColumn = 'campaign';
    }

    return `if(${tableAlias}.event_name in ('first_visit', 'first_open') and ${tableAlias}.${struct}.${structColumn} is null,
                ${tableAlias}.traffic_source.${userTrafficSourceColumn},
                if(${tableAlias}.collected_traffic_source.gclid is not null or ${tableAlias}.page_location like '%gbraid%' or ${tableAlias}.page_location like '%wbraid%',
                    if ('${structColumn}' in ('manual_campaign_name'),
                        if (${gadsCampaignName} is null,
                            '${trafficSource.cpcValue}',
                             ${gadsCampaignName}
                        ),
                        if('${structColumn}' in ('manual_source'),
                            ifnull(${tableAlias}.${struct}.${structColumn}, '${trafficSource.cpcValue}'),
                            '${trafficSource.cpcValue}'
                        )
                    ),
                    if(${tableAlias}.collected_traffic_source.dclid is not null and ${tableAlias}.collected_traffic_source.gclid is null,
                            '${trafficSource.cpmValue}',
                            ${tableAlias}.${struct}.${structColumn}
                    )
                )
            )`;
}

function adjust_campaign_timestamp(table_alias){
    let microseconds_in_second = 1000000;
    return `if(${table_alias}.event_name in ('firebase_campaign', 'campaign_details'),
                if((${table_alias}.event_timestamp/${microseconds_in_second}) <= (${table_alias}.ga_session_id + 4),
                    (${table_alias}.ga_session_id * ${microseconds_in_second}) - ((4 * ${microseconds_in_second}) - (${table_alias}.event_timestamp - (${table_alias}.ga_session_id * ${microseconds_in_second}))),
                    ${table_alias}.event_timestamp
                ),
                ${table_alias}.event_timestamp
            )`
}

function select_source_categories(){
    let select_statement = '';
    let i = 0;
    let { source_categories } = require("./source_categories.js");
    let source_categories_length = Object.keys(source_categories).length;
    for (row of source_categories) {
        i++;
        select_statement = select_statement.concat(`select "` + row.source + `" as source, "`+  row.source_category + `" as source_category`);
        if (i < source_categories_length) {
            select_statement = select_statement.concat(` union distinct `)
        }
    }
    return select_statement;
}

function select_non_custom_events(){
    let select_statement = '';
    let i = 0;
    let { non_custom_events } = require("./non_custom_events.js");
    let non_custom_events_length = Object.keys(non_custom_events).length;
    for (row of non_custom_events) {
        i++;
        select_statement = select_statement.concat(`select "` + row.event_name + `" as event_name`);
        if (i < non_custom_events_length) {
            select_statement = select_statement.concat(` union distinct `)
        }
    }
    return select_statement;
}


function select_is_key_events(){
    let select_statement = '';
    let i = 0;
    let { is_key_events } = require("./is_key_events.js");
    let is_key_events_length = Object.keys(is_key_events).length;
    for (row of is_key_events) {
        i++;
        select_statement = select_statement.concat(`select "` + row.event_name + `" as event_name`);
        if (i < is_key_events_length) {
            select_statement = select_statement.concat(` union distinct `)
        }
    }
    return select_statement;
}

module.exports = { unnest_column, extract_url, group_channels, updatePaidSearchTrafficSource, adjust_campaign_timestamp, select_source_categories, select_non_custom_events , select_is_key_events };