
REM Upload Affective go no-go
python -t data_rdmr_agn_t -f AffectiveGoNogo_datafileAffectiveGoNogo_datafile_*.iqdat -p "//wcs/wtp_common/data/RDoC Computer Tasks\Inquisit tasks - USE ME\Affective"

REM Upload Hungry Donkey
python -t data_rdmr_hdt_t -f HDT_raw_*.iqdat -p "//wcs/wtp_common/data/RDoC Computer Tasks/Inquisit tasks - USE ME/HungryDonkeyTask"

REM Upload psap
python -t data_rdmr_psap_t -f PSAP_rawdata_*.iqdat -p "//wcs/wtp_common/data/RDoC Computer Tasks/Inquisit tasks - USE ME/PSAP"