# wootag

Sample Beacons:
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491732332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491733332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fh56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491731332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fi56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491735332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}
{"vid":"fsdf345erfreg5","starttime":1491731813,"currenttime":1491734332,"videolength":3600,"uid":"frr435fg56ger"}


Hive Table:
CREATE EXTERNAL TABLE IF NOT EXISTS wootag.videoviews(
starttime bigint,
currenttime bigint,
videolength bigint,
uid string)
PARTITIONED BY (archived string, timepartition bigint, vid string) STORED AS PARQUET LOCATION '/wootag';
