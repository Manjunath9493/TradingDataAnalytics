raw_data_path = "/mnt/rawdata/"

NumOfDays = 365
from datetime import date, timedelta, datetime
ToDate = date.today()
FromDate = ToDate - timedelta(NumOfDays)

FromDate_formatted = f"{FromDate} 00:00"  
ToDate_formatted = f"{ToDate} 00:00"  

interval = "ONE_DAY"

SymToken_Symbol_pairs = {
10585:'MOM30IETF-EQ'
,10690:'NIFTYQLITY-EQ'
,25851:'VAL30IETF-EQ'
,23806:'ABSLPSE-EQ'
,9168:'UTISXN50-EQ'
,2328:'CPSEETF-EQ'
,14428:'GOLDBEES-EQ'
,18284:'HNGSNGBEES-EQ'
,7074:'MAHKTECH-EQ'
,11241:'HDFCGROWTH-EQ'
,21254:'LOWVOLIETF-EQ'
,11255:'HDFCQUAL-EQ'
,3001:'BSE500IETF-EQ'
,13198:'COMMOIETF-EQ'
,12578:'FINIETF-EQ'
,10723:'INFRAIETF-EQ'
,10676:'MNC-EQ'
,19640:'ALPHAETF-EQ'
,23855:'MIDSMALL-EQ'
,22832:'SMALLCAP-EQ'
,19237:'MONIFTY500-EQ'
,23184:'MOREALTY-EQ'
,23181:'MOSMALL250-EQ'
,10825:'MOVALUE-EQ'
,7422:'MONQ50-EQ'
,22739:'MON100-EQ'
,24081:'TOP100CASE-EQ'
,10576:'NIFTYBEES-EQ'
,25606:'MOMENTUM50-EQ'
,7412:'ALPHA-EQ'
,22344:'ALPL30IETF-EQ'
,7844:'AUTOIETF-EQ'
,11439:'BANKBEES-EQ'
,2636:'DIVOPPBEES-EQ'
,24461:'EVINDIA-EQ'
,5220:'BFSI-EQ'
,5306:'FMCGIETF-EQ'
,6297:'HEALTHY-EQ'
,10508:'MOHEALTH-EQ'
,2435:'CONSUMBEES-EQ'
,24944:'MODEFENCE-EQ'
,8882:'TNIDETF-EQ'
,7979:'MAKEINDIA-EQ'
,19084:'ITBEES-EQ'
,24861:'METALIETF-EQ'
,21423:'MOM100-EQ'
,8413:'MIDCAPETF-EQ'
,7456:'MIDQ50ADD-EQ'
,8077:'MIDCAP-EQ'
,4529:'NEXT50IETF-EQ'
,24533:'OILIETF-EQ'
,4973:'PHARMABEES-EQ'
,11386:'PVTBANIETF-EQ'
,15032:'PSUBNKBEES-EQ'
,25171:'TOP10ADD-EQ'
,1200:'ESG-EQ'
,17475:'NV20IETF-EQ'
,25080:'MULTICAP-EQ'
,25996:'EMULTIMQ-EQ'
,3507:'MAFANG-EQ'
,5782:'MASPTOP50-EQ'
,522:'ICICIB22-EQ'
,17702:'MIDSELIETF-EQ'
,8080:'SILVERBEES-EQ'
,4378:'SENSEXIETF-EQ'
,17044:'SHARIABEES-EQ'
}

