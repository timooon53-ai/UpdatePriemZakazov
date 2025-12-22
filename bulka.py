[SETTINGS]
{
  "Name": "заказ такси",
  "SuggestedBots": 1,
  "MaxCPM": 0,
  "LastModified": "2025-12-21T21:45:46.5022928+06:00",
  "AdditionalInfo": "",
  "RequiredPlugins": [],
  "Author": "",
  "Version": "1.1.2 [SB]",
  "SaveEmptyCaptures": false,
  "ContinueOnCustom": false,
  "SaveHitsToTextFile": false,
  "IgnoreResponseErrors": false,
  "MaxRedirects": 8,
  "NeedsProxies": false,
  "OnlySocks": false,
  "OnlySsl": false,
  "MaxProxyUses": 0,
  "BanProxyAfterGoodStatus": false,
  "BanLoopEvasionOverride": -1,
  "EncodeData": false,
  "AllowedWordlist1": "",
  "AllowedWordlist2": "",
  "DataRules": [],
  "CustomInputs": [],
  "CaptchaUrl": "",
  "IsBase64": false,
  "FilterList": [],
  "EvaluateMathOCR": false,
  "SecurityProtocol": 0,
  "ForceHeadless": false,
  "AlwaysOpen": false,
  "AlwaysQuit": false,
  "QuitOnBanRetry": false,
  "AcceptInsecureCertificates": true,
  "DisableNotifications": false,
  "DisableImageLoading": false,
  "DefaultProfileDirectory": false,
  "CustomUserAgent": "",
  "RandomUA": false,
  "CustomCMDArgs": "",
  "Title": "заказ такси",
  "IconPath": "Icon\\svbfile.ico",
  "LicenseSource": null,
  "Message": null,
  "MessageColor": "#FFFFFFFF",
  "HitInfoFormat": "[{hit.Type}][{hit.Proxy}] {hit.Data} - [{hit.CapturedString}]",
  "AuthorColor": "#FFFFB266",
  "WordlistColor": "#FFB5C2E1",
  "BotsColor": "#FFA8FFFF",
  "CustomInputColor": "#FFD6C7C7",
  "CPMColor": "#FFFFFFFF",
  "ProgressColor": "#FFAD93E3",
  "HitsColor": "#FF66FF66",
  "CustomColor": "#FFFFB266",
  "ToCheckColor": "#FF7FFFD4",
  "FailsColor": "#FFFF3333",
  "RetriesColor": "#FFFFFF99",
  "OcrRateColor": "#FF4698FD",
  "ProxiesColor": "#FFFFFFFF"
}

[SCRIPT]
PARSE "" LR "" "" -> VAR "token2" "y0_AgAAAAB1g7gdAAU0HAAAAAECOUIwAAAYjdKIuM9IEZ2DXVd1oG4LOWpPrg" "" 

#LAUNCH REQUEST POST "https://tc.taxi.yandex.net/3.0/launch" 
  CONTENT "{}" 
  CONTENTTYPE "application/json" 
  HEADER "User-Agent: com.yandex.lavka/1.6.0.49 go-platform/0.1.19 Android/" 
  HEADER "Pragma: no-cache" 
  HEADER "Accept: */*" 
  HEADER "Host: tc.mobile.yandex.net" 
  HEADER "Content-Type: application/json" 
  HEADER "Authorization: Bearer <token2>" 
  HEADER "x-oauth-token: <token2>" 

#A PARSE "" LR "" "" -> VAR "part_A" "молодова 20 Омск" "" 

#A REQUEST POST "https://tc.mobile.yandex.net/4.0/persuggest/v1/suggest?mobcf=russia%25go_ru_by_geo_hosts_2%25default&mobpr=go_ru_by_geo_hosts_2_TAXI_V4_0" 
  CONTENT "{\"type\":\"a\",\"part\":\"<part_A>\",\"client_reqid\":\"1764650675979_ebb57515c4883b271c4dce99ace5f11b\",\"session_info\":{},\"action\":\"user_input\",\"state\":{\"bbox\":[73.44446455010228,54.9072988605965,73.44655181916946,54.904995264809976],\"location_available\":false,\"coord_providers\":[],\"precise_location_available\":false,\"wifi_networks\":[],\"fields\":[{\"position\":[73.44550818463587,54.90664973530346],\"metrica_method\":\"pin_drop\",\"finalsuggest_method\":\"fs_not_sticky\",\"log\":\"{\\\"uri\\\":\\\"ymapsbm1:\\/\\/geo?data=Cgg1NzExODE5NhJv0KDQvtGB0YHQuNGPLCDQntC80YHQuiwg0LzQuNC60YDQvtGA0LDQudC-0L0g0JzQvtGB0LrQvtCy0LrQsC0yLCDRg9C70LjRhtCwINCv0YDQvtGB0LvQsNCy0LAg0JPQsNGI0LXQutCwLCAxMy8xIgoN_OOSQhVDoFtC\\\",\\\"trace_id\\\":\\\"dcf2c5d465ce4b918a3641547ceed8cb\\\"}\",\"metrica_action\":\"manual\",\"type\":\"a\"}],\"selected_class\":\"econom\",\"l10n\":{\"countries\":{\"system\":[\"RU\"]},\"languages\":{\"system\":[\"ru-RU\"],\"app\":[\"ru\"]},\"mapkit_lang_region\":\"ru_RU\"},\"app_metrica\":{\"uuid\":\"12dcca3de0be448c8efd4f2ab68bf007\",\"device_id\":\"818182718hffy\"},\"main_screen_version\":\"flex_main\",\"screen\":\"main.addresses\"},\"suggest_serpid\":\"8aa2d1a77c60db11e2fa8cac6016ac2a\"}" 
  CONTENTTYPE "application/json" 
  HEADER "User-Agent: ru.yandex.ytaxi/700.116.0.501961 (iPhone; iPhone13,2; iOS 18.6; Darwin)" 
  HEADER "Pragma: no-cache" 
  HEADER "Accept: */*" 
  HEADER "Accept-Encoding: gzip, deflate, br" 
  HEADER "X-YaTaxi-UserId: 08a2d06810664758a42dee25bb0220ec" 
  HEADER "X-Ya-Go-Superapp-Session: 06F16257-7919-4052-BB9A-B96D22FE9B79" 
  HEADER "X-YaTaxi-Last-Zone-Names: novosibirsk,moscow,omsk" 
  HEADER "X-Yandex-Jws: eyJhbGciOiJIUzI1NiIsImtpZCI6Im5hcndoYWwiLCJ0eXAiOiJKV1QifQ.eyJkZXZpY2VfaW50ZWdyaXR5Ijp0cnVlLCJleHBpcmVzX2F0X21zIjoxNzY0NjUzNzcyNDY4LCJpcCI6IjJhMDI6NmI4OmMzNzo4YmE5OjdhMDA6NGMxYjozM2Q3OjAiLCJ0aW1lc3RhbXBfbXMiOjE3NjQ2NTAxNzI0NjgsInV1aWQiOiIxMmRjY2EzZGUwYmU0NDhjOGVmZDRmMmFiNjhiZjAwNyJ9.H8Izcf7uXk80ZFVKRElhDyabqcBVKTMsa45oeXQmgIs" 
  HEADER "Content-Length: 1113" 
  HEADER "X-Perf-Class: medium" 
  HEADER "Date: Tue, 02 Dec 2025 04:44:36 GMT" 
  HEADER "Connection: keep-alive" 
  HEADER "Authorization: Bearer <token2>" 
  HEADER "Accept-Language: ru;q=1, ru-RU;q=0.9" 
  HEADER "X-Yataxi-Ongoing-Orders-Statuses: none" 
  HEADER "Content-Type: application/json" 
  HEADER "X-VPN-Active: 1" 
  HEADER "X-Mob-ID: c76e6e2552f348b898891dd672fa5daa" 
  HEADER "X-YaTaxi-Has-Ongoing-Orders: false" 

#log_a PARSE "<SOURCE>" LR ",\"log\":\"{" "}\",\"" -> VAR "log_a" 

#title_a PARSE "<SOURCE>" LR "\"title\":{\"text\":\"" "\",\"hl\"" -> VAR "title_a" 

#point_A PARSE "<SOURCE>" LR "{\"point\":" "}," Recursive=TRUE -> VAR "point_A" 

#1_подъезд PARSE "<point_A>" LR "[[" "],\"" -> VAR "1" 

#B PARSE "" LR "" "" -> VAR "part_B" "3-я ленинградская 43 Омск" "" 

#B REQUEST POST "https://tc.mobile.yandex.net/4.0/persuggest/v1/suggest?mobcf=russia%25go_ru_by_geo_hosts_2%25default&mobpr=go_ru_by_geo_hosts_2_TAXI_V4_0" 
  CONTENT "{\"action\":\"user_input\",\"suggest_serpid\":\"1fffc1028b7f9f7bfc80b0ac30417df1\",\"client_reqid\":\"1764651135479_cd7cb200336a407eba8b5cd895cbe44c\",\"part\":\"<part_B>\",\"session_info\":{},\"state\":{\"selected_class\":\"econom\",\"coord_providers\":[],\"fields\":[{\"entrance\":\"4\",\"metrica_method\":\"suggest\",\"position\":[37.63283473672819,55.81002045183566],\"log\":\"{\\\"suggest_reqid\\\":\\\"1764650676398765-287523944-suggest-maps-yp-22\\\",\\\"user_params\\\":{\\\"request\\\":\\\"Бочкова 5\\\",\\\"ll\\\":\\\"73.445511,54.906147\\\",\\\"spn\\\":\\\"0.00208282,0.00230408\\\",\\\"ull\\\":\\\"73.445511,54.906147\\\",\\\"lang\\\":\\\"ru\\\"},\\\"client_reqid\\\":\\\"1764650675979_ebb57515c4883b271c4dce99ace5f11b\\\",\\\"server_reqid\\\":\\\"1764650676398765-287523944-suggest-maps-yp-22\\\",\\\"pos\\\":0,\\\"type\\\":\\\"toponym\\\",\\\"where\\\":{\\\"name\\\":\\\"Россия, Москва, улица Бочкова, 5\\\",\\\"source_id\\\":\\\"56760816\\\",\\\"mutable_source_id\\\":\\\"56760816\\\",\\\"title\\\":\\\"улица Бочкова, 5\\\"},\\\"uri\\\":\\\"ymapsbm1:\\/\\/geo?data=Cgg1Njc2MDgxNhI40KDQvtGB0YHQuNGPLCDQnNC-0YHQutCy0LAsINGD0LvQuNGG0LAg0JHQvtGH0LrQvtCy0LAsIDUiCg3whxZCFYY9X0I,\\\",\\\"method\\\":\\\"suggest.geosuggest\\\",\\\"trace_id\\\":\\\"cb7de160c386df3ca6958bfd5850e8eb\\\"}\",\"type\":\"a\",\"finalsuggest_method\":\"np_entrances\"}],\"l10n\":{\"countries\":{\"system\":[\"RU\"]},\"languages\":{\"app\":[\"ru\"],\"system\":[\"ru-RU\"]},\"mapkit_lang_region\":\"ru_RU\"},\"bbox\":[37.63176701134504,55.81066951258319,37.63390246211134,55.80836614425004],\"screen\":\"main.addresses\",\"main_screen_version\":\"flex_main\",\"location_available\":false,\"app_metrica\":{\"device_id\":\"818182718hffy\",\"uuid\":\"12dcca3de0be448c8efd4f2ab68bf007\"},\"precise_location_available\":false,\"wifi_networks\":[]},\"type\":\"b\"}" 
  CONTENTTYPE "application/json" 
  HEADER "Accept-Encoding: gzip, deflate, br" 
  HEADER "User-Agent: ru.yandex.ytaxi/700.116.0.501961 (iPhone; iPhone13,2; iOS 18.6; Darwin)" 
  HEADER "X-YaTaxi-UserId: 08a2d06810664758a42dee25bb0220ec" 
  HEADER "X-Ya-Go-Superapp-Session: 06F16257-7919-4052-BB9A-B96D22FE9B79" 
  HEADER "X-YaTaxi-Last-Zone-Names: novosibirsk,moscow,omsk" 
  HEADER "X-Yandex-Jws: eyJhbGciOiJIUzI1NiIsImtpZCI6Im5hcndoYWwiLCJ0eXAiOiJKV1QifQ.eyJkZXZpY2VfaW50ZWdyaXR5Ijp0cnVlLCJleHBpcmVzX2F0X21zIjoxNzY0NjUzNzcyNDY4LCJpcCI6IjJhMDI6NmI4OmMzNzo4YmE5OjdhMDA6NGMxYjozM2Q3OjAiLCJ0aW1lc3RhbXBfbXMiOjE3NjQ2NTAxNzI0NjgsInV1aWQiOiIxMmRjY2EzZGUwYmU0NDhjOGVmZDRmMmFiNjhiZjAwNyJ9.H8Izcf7uXk80ZFVKRElhDyabqcBVKTMsa45oeXQmgIs" 
  HEADER "Content-Length: 1113" 
  HEADER "X-Perf-Class: medium" 
  HEADER "Date: Tue, 02 Dec 2025 04:44:36 GMT" 
  HEADER "Connection: keep-alive" 
  HEADER "Authorization: Bearer <token2>" 
  HEADER "Accept-Language: ru;q=1, ru-RU;q=0.9" 
  HEADER "Accept: */*" 
  HEADER "X-Yataxi-Ongoing-Orders-Statuses: none" 
  HEADER "Content-Type: application/json" 
  HEADER "X-VPN-Active: 1" 
  HEADER "X-Mob-ID: c76e6e2552f348b898891dd672fa5daa" 
  HEADER "X-YaTaxi-Has-Ongoing-Orders: false" 

#log_b PARSE "<SOURCE>" LR ",\"log\":\"{" "}\",\"" -> VAR "log_b" 

#title_b PARSE "<SOURCE>" LR "\"title\":{\"text\":\"" "\",\"hl\"" -> VAR "title_b" 

#point_b PARSE "<SOURCE>" LR "\"position\":[" "],\"subtitle\"" -> VAR "point_b" 

#KLASS PARSE "" LR "" "" -> VAR "class" "comfortplus" "" 

#routestat REQUEST POST "https://tc.mobile.yandex.net/3.0/routestats?mobcf=russia%25go_ru_by_geo_hosts_2%25default&mobpr=go_ru_by_geo_hosts_2_TAXI_0" 
  CONTENT "{\"supports_verticals_selector\":true,\"id\":\"08a2d06810664758a42dee25bb0220ec\",\"supported_markup\":\"tml-0.1\",\"selected_class\":\"econom\",\"supported_verticals\":[\"drive\",\"transport\",\"hub\",\"intercity\",\"maas\",\"taxi\",\"ultima\",\"child\",\"delivery\",\"rest_tariffs\"],\"supports_no_cars_available\":true,\"supports_unavailable_alternatives\":true,\"suggest_alternatives\":true,\"skip_estimated_waiting\":false,\"supports_paid_options\":true,\"supports_explicit_antisurge\":true,\"parks\":[],\"is_lightweight\":false,\"tariff_requirements\":[{\"class\":\"econom\",\"requirements\":{}},{\"class\":\"lite_b2b\",\"requirements\":{}},{\"class\":\"business\",\"requirements\":{}},{\"class\":\"standart_b2b\",\"requirements\":{}},{\"class\":\"comfortplus\",\"requirements\":{}},{\"class\":\"optimum_b2b\",\"requirements\":{}},{\"class\":\"vip\",\"requirements\":{}},{\"class\":\"ultimate\",\"requirements\":{}},{\"class\":\"maybach\",\"requirements\":{}},{\"class\":\"child_tariff\",\"requirements\":{}},{\"class\":\"minivan\",\"requirements\":{}},{\"class\":\"premium_van\",\"requirements\":{}},{\"class\":\"personal_driver\",\"requirements\":{}},{\"class\":\"express\",\"requirements\":{}},{\"class\":\"courier\",\"requirements\":{}},{\"class\":\"cargo\",\"requirements\":{}},{\"class\":\"selfdriving\",\"requirements\":{}}],\"enable_fallback_for_tariffs\":true,\"supported\":[{\"type\":\"formatted_prices\"},{\"type\":\"multiclass_requirements\"},{\"type\":\"multiclasses\"},{\"type\":\"verticals_multiclass\",\"payload\":{\"classes\":[\"courier\",\"cargo\",\"ndd\",\"express_d2d\",\"express_outdoor\",\"express_d2d_slow\",\"sdd_short\",\"sdd_evening\",\"sdd_long\",\"express_fast\"]}},{\"type\":\"plus_promo_alternative\"},{\"type\":\"order_flow_delivery\",\"payload\":{\"classes\":[\"courier\",\"cargo\",\"ndd\",\"express_d2d\",\"express_outdoor\",\"express_d2d_slow\",\"sdd_short\",\"sdd_evening\",\"sdd_long\",\"express_fast\"]}},{\"type\":\"requirements_v2\"}],\"with_title\":true,\"supports_multiclass\":true,\"supported_vertical_types\":[\"group\"],\"supported_features\":[{\"type\":\"order_button_actions\",\"values\":[\"open_tariff_card\",\"deeplink\"]},{\"type\":\"swap_summary\",\"values\":[\"high_tariff_selector\"]}],\"delivery_extra\":{\"door_to_door\":false,\"is_delivery_business_account_enabled\":false,\"insurance\":{\"selected\":false},\"pay_on_delivery\":false},\"route\":[[<1>],[<point_b>]],\"payment\":{\"type\":\"cash\"},\"zone_name\":\"moscow\",\"account_type\":\"lite\",\"summary_version\":2,\"format_currency\":true,\"supports_hideable_tariffs\":true,\"force_soon_order\":false,\"use_toll_roads\":false,\"estimate_waiting_selected_only\":false,\"selected_class_only\":false,\"position_accuracy\":0,\"size_hint\":300,\"extended_description\":true,\"requirements\":{},\"multiclass_options\":{\"selected\":false,\"class\":[],\"verticals\":[]}}" 
  CONTENTTYPE "application/json" 
  HEADER "X-YaTaxi-UserId: 08a2d06810664758a42dee25bb0220ec" 
  HEADER "User-Agent: ru.yandex.ytaxi/700.116.0.501961 (iPhone; iPhone13,2; iOS 18.6; Darwin)" 
  HEADER "X-YaTaxi-Has-Ongoing-Orders: false" 
  HEADER "X-Ya-Go-Superapp-Session: 06F16257-7919-4052-BB9A-B96D22FE9B79" 
  HEADER "X-YaTaxi-Last-Zone-Names: novosibirsk,omsk,moscow" 
  HEADER "X-Yandex-Jws: eyJhbGciOiJIUzI1NiIsImtpZCI6Im5hcndoYWwiLCJ0eXAiOiJKV1QifQ.eyJkZXZpY2VfaW50ZWdyaXR5Ijp0cnVlLCJleHBpcmVzX2F0X21zIjoxNzY0NjUzNzcyNDY4LCJpcCI6IjJhMDI6NmI4OmMzNzo4YmE5OjdhMDA6NGMxYjozM2Q3OjAiLCJ0aW1lc3RhbXBfbXMiOjE3NjQ2NTAxNzI0NjgsInV1aWQiOiIxMmRjY2EzZGUwYmU0NDhjOGVmZDRmMmFiNjhiZjAwNyJ9.H8Izcf7uXk80ZFVKRElhDyabqcBVKTMsa45oeXQmgIs" 
  HEADER "Content-Length: 6510" 
  HEADER "X-Perf-Class: medium" 
  HEADER "Connection: keep-alive" 
  HEADER "Authorization: Bearer <token2>" 
  HEADER "Accept-Language: ru;q=1, ru-RU;q=0.9" 
  HEADER "Accept: */*" 
  HEADER "X-Yataxi-Ongoing-Orders-Statuses: none" 
  HEADER "Content-Type: application/json" 
  HEADER "X-VPN-Active: 1" 
  HEADER "Accept-Encoding: gzip, deflate, br" 
  HEADER "X-Mob-ID: c76e6e2552f348b898891dd672fa5daa" 

#price PARSE "<SOURCE>" REGEX "\"pin_description\"\\s*:\\s*\"Отсюда[\\s\\u00A0\\u202F]*([0-9]+)[^\"]*\"\\s*,\\s*\"class\"\\s*:\\s*\"<class>\"" "[1]" -> VAR "price" 

#classSS PARSE "<SOURCE>" LR "\"class\":\"<class>\",\"name\":\"" "\"" -> VAR "classss" 

#govno PARSE "<SOURCE>" LR "\"class\":\"<class>\",\"nam" "\"is_hidden\":" -> VAR "govno" 

#offer PARSE "<govno>" LR "\"offer\":\"" "\"," -> VAR "offer" 

REQUEST POST "https://tc.mobile.yandex.net/3.0/orderdraft?mobcf=russia%25go_ru_by_geo_hosts_2%25default&mobpr=go_ru_by_geo_hosts_2_TAXI_0" 
  CONTENT "{\"id\":\"8f289b4834494ec08744d142e8878b61\",\"zone_name\":\"omsk\",\"payment\":{\"type\":\"cash\"},\"parks\":[],\"vertical_id\":\"taxi\",\"dont_call\":false,\"forced_surge\":{\"value\":2},\"client_geo_sharing_enabled\":false,\"supported\":[\"code_dispatch\",\"requirements_v2\"],\"driverclientchat_enabled\":true,\"tips\":{\"type\":\"percent\",\"decimal_value\":\"0\"},\"last_seen_offer\":\"118cbaa4b5a5b92c3a61a1484b907947\",\"location\":[73.445458314433736,54.905743057958546],\"route\":[{\"exact\":true,\"description\":\"микрорайон Московка-2, Омск\",\"short_text\":\"Молодова 20, подъезд 7\",\"fullname\":\"Омск, микрорайон Московка-2, улица Молодова, 20, подъезд 7\",\"uri\":\"ymapsbm1:\\/\\/geo?data=Cgk3NzIwMDI4ODUSctCg0L7RgdGB0LjRjywg0J7QvNGB0LosINC80LjQutGA0L7RgNCw0LnQvtC9INCc0L7RgdC60L7QstC60LAtMiwg0YPQu9C40YbQsCDQnNC-0LvQvtC00L7QstCwLCAyMCwg0L_QvtC00YrQtdC30LQgNyIKDVjkkkIVR59bQjDAl_ivBg,,\",\"metrica_method\":\"pin_drop\",\"log\":\"{\\\"uri\\\":\\\"ymapsbm1:\\/\\/geo?data=Cgk3NzIwMDI4ODUSctCg0L7RgdGB0LjRjywg0J7QvNGB0LosINC80LjQutGA0L7RgNCw0LnQvtC9INCc0L7RgdC60L7QstC60LAtMiwg0YPQu9C40YbQsCDQnNC-0LvQvtC00L7QstCwLCAyMCwg0L_QvtC00YrQtdC30LQgNyIKDVjkkkIVR59bQjDAl_ivBg,,\\\",\\\"point_extra_details\\\":{\\\"entrance\\\":\\\"7\\\"},\\\"trace_id\\\":\\\"82aef9ac5a16d888c4d74752ad3ca0ff\\\"}\",\"thoroughfare\":\"улица Молодова\",\"type\":\"address\",\"metrica_action\":\"auto\",\"premisenumber\":\"20\",\"porchnumber\":\"7\",\"use_geopoint\":false,\"locality\":\"Омск\",\"geopoint\":[73.44561982576036,54.905580887768814]},{\"exact\":true,\"description\":\"Омск\",\"metrica_method\":\"suggest\",\"short_text\":\"Куйбышева 134к1\",\"uri\":\"ymapsbm1:\\/\\/geo?data=Cgg1NzEzOTg2NRI90KDQvtGB0YHQuNGPLCDQntC80YHQuiwg0YPQu9C40YbQsCDQmtGD0LnQsdGL0YjQtdCy0LAsIDEzNNC6MSIKDQjMkkIVUeRbQg,,\",\"log\":\"{\\\"uri\\\":\\\"ymapsbm1:\\/\\/geo?data=Cgg1NzEzOTg2NRI90KDQvtGB0YHQuNGPLCDQntC80YHQuiwg0YPQu9C40YbQsCDQmtGD0LnQsdGL0YjQtdCy0LAsIDEzNNC6MSIKDQjMkkIVUeRbQg,,\\\",\\\"is_arrival_point\\\":true,\\\"originals\\\":{\\\"position\\\":[73.39839539127418,54.972876370424984],\\\"uri\\\":\\\"ymapsbm1:\\/\\/geo?data=Cgg1NzEzOTg2NRI90KDQvtGB0YHQuNGPLCDQntC80YHQuiwg0YPQu9C40YbQsCDQmtGD0LnQsdGL0YjQtdCy0LAsIDEzNNC6MSIKDQjMkkIVUeRbQg,,\\\"},\\\"trace_id\\\":\\\"ebb38997ef0090d02c83d5a4e1c18fbe\\\"}\",\"thoroughfare\":\"улица Куйбышева\",\"type\":\"address\",\"premisenumber\":\"134к1\",\"fullname\":\"Омск, улица Куйбышева, 134к1\",\"use_geopoint\":false,\"locality\":\"Омск\",\"geopoint\":[73.398221145827947,54.972940041672899]}],\"driverclientchat_translate_enabled\":true,\"personal_wallet_enabled\":true,\"requirements\":{},\"dont_sms\":true,\"class\":[\"econom\"]}" 
  CONTENTTYPE "application/json" 
  HEADER "Host: tc.mobile.yandex.net" 
  HEADER "X-Ya-Go-Superapp-Session: CD8DD04E-FB2D-4EF1-A8BF-04BD4F3B530F" 
  HEADER "Authorization: Bearer <token2>" 
  HEADER "X-Yataxi-Ongoing-Orders-Statuses: taxi=unknown@1" 
  HEADER "Accept: */*" 
  HEADER "X-Yandex-Jws: err_safetynet_access" 
  HEADER "X-VPN-Active: 1" 
  HEADER "Accept-Language: ru;q=1, ru-RU;q=0.9" 
  HEADER "X-YaTaxi-Last-Zone-Names: mytishchi,moscow,omsk" 
  HEADER "Accept-Encoding: gzip, deflate, br" 
  HEADER "X-Mob-ID: 31883e036139466da7290d15c6fc3f30" 
  HEADER "X-YaTaxi-Has-Ongoing-Orders: true" 
  HEADER "User-Agent: yandex-taxi/4.11.0.127882 Android/9 (Asus; ASUS_I003DD)" 
  HEADER "Content-Length: 2576" 
  HEADER "Connection: keep-alive" 
  HEADER "X-YaTaxi-UserId: 8f289b4834494ec08744d142e8878b61" 

PARSE "<SOURCE>" LR "" "" -> VAR "" 

