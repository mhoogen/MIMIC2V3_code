import pandas as pd
import numpy as np
from pandasql import sqldf

# pysqldf = lambda q: sqldf(q, globals())

count = 0
tmp = np.arange(count, 33)
tmp = [str(i) for i in tmp]
tmp[0:10] = ['0' + i for i  in tmp[0:10]]
file_syntax = 'mimic_pandas_db-'
file_ending = '.pkl'
path_syntax = 'C:/Users/ali_e_000/Desktop/Research Paper Business Analytics/data/picklefiles/'
fullpaths = [path_syntax + file_syntax + i + file_ending for i in tmp]

# Start the loop
for pklpath in fullpaths:
    print 'Reading in partition name: %s' % pklpath

    partition_db = pd.read_pickle(pklpath)

    print 'Executing queries'
    agedata  = """
    SELECT i.subject_id
      FROM ICUSTAY_DETAIL i
      WHERE i.ICUSTAY_ADMIT_AGE < 99
        AND i.ICUSTAY_ADMIT_AGE > 1
    """

    chartview = """
      SELECT ce.subject_id, ce.charttime,
        Max((Case When ce.itemid = 198 OR ce.itemid = 220742 Then round(ce.value1num,2)  Else cast('' as numeric) End)) AS GCS,
        Max((Case When ce.itemid = 581 OR ce.itemid = 224639 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Weight,
        Max((Case When ce.itemid = 762 OR ce.itemid = 226512 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS AdmitWt,
        Max((Case When ce.itemid = 4188 OR ce.itemid = 226730 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Height,

        Max((Case When ce.itemid = 455 OR ce.itemid = 220179 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS NBPSys,
        Max((Case When ce.itemid = 455 OR ce.itemid = 220180 Then round(ce.value2num,2) Else cast('' as numeric) End)) AS NBPDias,
        Max((Case When ce.itemid = 456 OR ce.itemid = 220181 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS NBPMean,
        Max((Case When ce.itemid = 1149 OR ce.itemid = 224324 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS NBP,
        Max((Case When ce.itemid = 51 OR ce.itemid = 220050 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS SBP,
        Max((Case When ce.itemid = 51 OR ce.itemid = 220051 Then round(ce.value2num,2) Else cast('' as numeric) End)) AS DBP,
        Max((Case When ce.itemid = 52 OR ce.itemid = 220052 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS MAP,
        Max((Case When ce.itemid = 211 OR ce.itemid = 220045 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS HR,
        Max((Case When ce.itemid = 211 OR ce.itemid = 220045 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS RESP,
        Max((Case When ce.itemid = 646 OR ce.itemid = 228232 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS SpO2,
        Max((Case When ce.itemid = 113 OR ce.itemid = 220074 OR ce.itemid = 1103 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS CVP,
        Max((Case When ce.itemid = 491 OR ce.itemid = 220061 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS PAPMean,
        Max((Case When ce.itemid = 492 OR ce.itemid = 220059 OR ce.itemid = 220059 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS PAPSd,
        Max((Case When ce.itemid = 116 OR ce.itemid = 220093 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS CrdIndx,
        Max((Case When ce.itemid = 626 OR ce.itemid = 1373 OR ce.itemid = 226262 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS SVR,
        Max((Case When ce.itemid = 90 OR ce.itemid = 220088 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS COtd,
        Max((Case When ce.itemid = 89 OR ce.itemid = 226263 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS COfck,
        Max((Case When ce.itemid = 504 OR ce.itemid = 223771 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS PCWP,
        Max((Case When ce.itemid = 512 OR ce.itemid = 226863 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS PVR,

        Max((Case When ce.itemid = 837 OR ce.itemid = 1536 OR ce.itemid = 220645 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Na,
        Max((Case When ce.itemid = 829 OR ce.itemid = 1535 OR ce.itemid = 227442 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS K,
        Max((Case When ce.itemid = 788 OR ce.itemid = 1523 OR ce.itemid = 220602 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Cl,
        Max((Case When ce.itemid = 787 OR ce.itemid = 225698 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS CO2,
        Max((Case When ce.itemid = 811 OR ce.itemid = 228388 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Glucose,
        Max((Case When ce.itemid = 781 OR ce.itemid = 1162 OR ce.itemid = 225624 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS BUN,
        Max((Case When ce.itemid = 791 OR ce.itemid = 1525 OR ce.itemid = 220615 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Creatinine,
        Max((Case When ce.itemid = 821 OR ce.itemid = 1532 OR ce.itemid = 220635 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Mg,
        Max((Case When ce.itemid = 770 OR ce.itemid = 220587 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS AST,
        Max((Case When ce.itemid = 769 OR ce.itemid = 220644 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS ALT,
        Max((Case When ce.itemid = 786 OR ce.itemid = 1522 OR ce.itemid = 225625 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Ca,
        Max((Case When ce.itemid = 816 OR ce.itemid = 225667 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS IonCa,
        Max((Case When ce.itemid = 848 OR ce.itemid = 1538 OR ce.itemid = 225690 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS TBili,
        Max((Case When ce.itemid = 803 OR ce.itemid = 1527 OR ce.itemid = 225651 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS DBili,
        Max((Case When ce.itemid = 849 OR ce.itemid = 1539 OR ce.itemid = 220650 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS TProtein,
        Max((Case When ce.itemid = 772 OR ce.itemid = 1521 OR ce.itemid = 227456 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Albumin,
        Max((Case When ce.itemid = 818 OR ce.itemid = 1531 OR ce.itemid = 225668 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Lactate,
        Max((Case When ce.itemid = 851 OR ce.itemid = 227429 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Troponin,

        Max((Case When ce.itemid = 813 OR ce.itemid = 226540 Then round(ce.value1num,2)  Else cast('' as numeric) End)) AS HCT,
        Max((Case When ce.itemid = 814 OR ce.itemid = 220228 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Hg,
        Max((Case When ce.itemid = 828 OR ce.itemid = 225170 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS Platelets,
        Max((Case When ce.itemid = 815 OR ce.itemid = 1530 OR ce.itemid = 227467 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS INR,
        Max((Case When ce.itemid = 824 OR ce.itemid = 1286 OR ce.itemid = 227465 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS PT,
        Max((Case When ce.itemid = 825 OR ce.itemid = 1533 OR ce.itemid = 227466 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS PTT,
        Max((Case When ce.itemid = 861 OR ce.itemid = 1127 OR ce.itemid = 1542 OR ce.itemid = 226734 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS WBC,
        Max((Case When ce.itemid = 833 OR ce.itemid = 225168 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS RBC,
        Max((Case When ce.itemid = 678 OR ce.itemid = 679 OR ce.itemid = 226182 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS TEMP,

        Max((Case When ce.itemid = 776 OR ce.itemid = 224828 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS ArtBE,
        Max((Case When ce.itemid = 777 OR ce.itemid = 225698 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS ArtCO2,
        Max((Case When ce.itemid = 778 OR ce.itemid = 220235 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS ArtPaCO2,
        Max((Case When ce.itemid = 779 OR ce.itemid = 220224 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS ArtPaO2,
        Max((Case When ce.itemid = 780 OR ce.itemid = 1126 OR ce.itemid = 223830 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS ArtpH,

        Max((Case When ce.itemid = 190 OR ce.itemid = 223835 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS FiO2Set,
        Max((Case When ce.itemid = 506 OR ce.itemid = 220339 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS PEEPSet,
        Max((Case When ce.itemid = 615 OR ce.itemid = 224690 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS RespTot,
        Max((Case When ce.itemid = 619 OR ce.itemid = 224688 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS RespSet,
        Max((Case When ce.itemid = 614 OR ce.itemid = 224689 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS RespSpon,
        Max((Case When ce.itemid = 535 OR ce.itemid = 224695 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS PIP,
        Max((Case When ce.itemid = 543 OR ce.itemid = 224696 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS PlateauPres,
        Max((Case When ce.itemid = 682 OR ce.itemid = 224685 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS TidVolObs,
        Max((Case When ce.itemid = 683 OR ce.itemid = 224684 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS TidVolSet,
        Max((Case When ce.itemid = 684 OR ce.itemid = 224686 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS TidVolSpon,
        Max((Case When ce.itemid = 834 OR ce.itemid = 224686 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS SaO2,

        Max((Case When ce.itemid = 212 OR ce.itemid = 220048 Then ce.value1  Else '' End)) AS Heart_Rhythm,
        Max((Case When ce.itemid = 161 OR ce.itemid = 224650 Then ce.value1  Else '' End)) AS Ectopy_Type,
        Max((Case When ce.itemid = 159 OR ce.itemid = 226478 Then ce.value1  Else '' End)) AS Ectopy_Freq,
        Max((Case When ce.itemid = 128 OR ce.itemid = 223758 Then ce.value1  Else '' End)) AS Code_Status,
        Max((Case When ce.itemid = 1484 OR ce.itemid = 223754 Then ce.value1  Else '' End)) AS FallRisk,
        Max((Case When ce.itemid = 479 OR ce.itemid = 223898 Then ce.value1  Else '' End)) AS Orientation,
        Max((Case When ce.itemid = 1337 OR ce.itemid = 223753 Then ce.value1  Else '' End)) AS RikerSAS,
        Max((Case When ce.itemid = 722 OR ce.itemid = 223848 Then ce.value1  Else '' End)) AS Vent,
        Max((Case When ce.itemid = 720 OR ce.itemid = 223849 Then ce.value1  Else '' End)) AS VentMode,
        Max((Case When ce.itemid = 516 OR ce.itemid = 223955 Then ce.value1  Else '' End)) AS Pacemaker,
        Max((Case When ce.itemid = 690 OR ce.itemid = 224831 Then ce.value1  Else '' End)) AS Trach,
        Max((Case When ce.itemid = 643 OR ce.itemid = 224028 Then ce.value1  Else '' End)) AS SkinColor,
        Max((Case When ce.itemid = 644 OR ce.itemid = 224026 Then ce.value1  Else '' End)) AS SkinIntegrity,
        Max((Case When ce.itemid = 225 OR ce.itemid = 225778 Then ce.value1  Else '' End)) AS IABP_Setting,
        Max((Case When ce.itemid = 1125 OR ce.itemid = 224640 Then ce.value1  Else '' End)) AS ServiceType,

        Max((Case When ce.itemid = 20001 Then round(ce.value1num,2) Else cast('' as numeric) End)) AS SAPS
      FROM CHARTEVENTS ce
      GROUP BY ce.subject_id, ce.charttime
    """

    test = """
      SELECT AGEDATA.icustay_admit_age
      FROM AGEDATA
      INNER JOIN CHARTVIEW
      ON AGEDATA.subject_id = CHARTVIEW.subject_id
    """

    med_view = """
      SELECT me.subject_id, me.charttime,
      Max((case when me.itemid in ('110', '225157') then 1 else cast('' as numeric) end)) as Aggrestat,
      Max((case when me.itemid in ('3', '221342') then 1 else cast('' as numeric) end)) as Aminophylline,
      Max((case when me.itemid in ('112', '221347') then 1 else cast('' as numeric) end)) as Amiodarone,
      Max((case when me.itemid = '40' then 1 else cast('' as numeric) end)) as Amrinone,
      Max((case when me.itemid in ('173', '225147') then 1 else cast('' as numeric) end)) as Argatroban,
      Max((case when me.itemid in ('141', '228314', '221385') then 1 else cast('' as numeric) end)) as Ativan,
      Max((case when me.itemid = '113' then 1 else cast('' as numeric) end)) as Atracurium,
      Max((case when me.itemid in ('174', '225148') then 1 else cast('' as numeric) end)) as Bivalirudin,
      Max((case when me.itemid in ('114', '221555') then 1 else cast('' as numeric) end)) as Cistracurium,
      Max((case when me.itemid in ('163', '221833') then 1 else cast('' as numeric) end)) as Dilaudid,
      Max((case when me.itemid in ('115', '221468') then 1 else cast('' as numeric) end)) as Diltiazem,
      Max((case when me.itemid in ('42', '221653') then 1 else cast('' as numeric) end)) as Dobutamine,
      Max((case when me.itemid in ('43', '221662') then 1 else cast('' as numeric) end)) as Dopamine,
      Max((case when me.itemid = '116' then 1 else cast('' as numeric) end)) as Doxacurium,
      Max((case when me.itemid in ('44', '221289') then 1 else cast('' as numeric) end)) as Epinephrine,
      Max((case when me.itemid = '119' then 1 else cast('' as numeric) end)) as EpinephrineK,
      Max((case when me.itemid in ('117', '221429') then 1 else cast('' as numeric) end)) as Esmolol,
      Max((case when me.itemid in ('149', '225942') then 1 else cast('' as numeric) end)) as FentanylConc,
      Max((case when me.itemid in ('118', '221744') then 1 else cast('' as numeric) end)) as Fentanyl,
      Max((case when me.itemid in ('25', '225152') then 1 else cast('' as numeric) end)) as Heparin,
      Max((case when me.itemid in ('45', '223258') then 1 else cast('' as numeric) end)) as Insulin,
      Max((case when me.itemid = '142' then 1 else cast('' as numeric) end)) as Integrelin,
      Max((case when me.itemid in ('151', '221712') then 1 else cast('' as numeric) end)) as Ketamine,
      Max((case when me.itemid = '122' then 1 else cast('' as numeric) end)) as Labetolol,
      Max((case when me.itemid in ('123', '152', '221794', '228340') then 1 else cast('' as numeric) end)) as Lasix,
      Max((case when me.itemid in ('177', '221892') then 1 else cast('' as numeric) end)) as Lepirudin,
      Max((case when me.itemid = '47' then 1 else cast('' as numeric) end)) as Levophed,
      Max((case when me.itemid = '120' then 1 else cast('' as numeric) end)) as LevophedK,
      Max((case when me.itemid in ('48', '225945') then 1 else cast('' as numeric) end)) as Lidocaine,
      Max((case when me.itemid in ('124', '221668') then 1 else cast('' as numeric) end)) as Midazolam,
      Max((case when me.itemid in ('125', '221986') then 1 else cast('' as numeric) end)) as Miltinone,
      Max((case when me.itemid in ('126', '225154') then 1 else cast('' as numeric) end)) as MorphineSolfate,
      Max((case when me.itemid in ('148', '222021') then 1 else cast('' as numeric) end)) as Narcan,
      Max((case when me.itemid = '172' then 1 else cast('' as numeric) end)) as Natrecor,
      Max((case when me.itemid = '127' then 1 else cast('' as numeric) end)) as Neosynephrine,
      Max((case when me.itemid = '128' then 1 else cast('' as numeric) end)) as NeosynephrineK,
      Max((case when me.itemid in ('178', '222042') then 1 else cast('' as numeric) end)) as Nicardipine,
      Max((case when me.itemid in ('49', '222056') then 1 else cast('' as numeric) end)) as Nitroglycerine,
      Max((case when me.itemid = '121' then 1 else cast('' as numeric) end)) as NitroglycerineK,
      Max((case when me.itemid in ('50', '222051') then 1 else cast('' as numeric) end)) as Nitroprusside,
      Max((case when me.itemid = '129' then 1 else cast('' as numeric) end)) as Pancuronium,
      Max((case when me.itemid in ('130', '225156') then 1 else cast('' as numeric) end)) as Pentobarbitol,
      Max((case when me.itemid in ('167', '225150') then 1 else cast('' as numeric) end)) as Precedex,
      Max((case when me.itemid in ('52', '222151') then 1 else cast('' as numeric) end)) as Procainamide,
      Max((case when me.itemid in ('131', '222168') then 1 else cast('' as numeric) end)) as Propofol,
      Max((case when me.itemid in ('134', '221261') then 1 else cast('' as numeric) end)) as Reopro,
      Max((case when me.itemid = '133' then 1 else cast('' as numeric) end)) as Sandostatin,
      Max((case when me.itemid in ('135', '221319') then 1 else cast('' as numeric) end)) as TPA,
      Max((case when me.itemid in ('51', '222315') then 1 else cast('' as numeric) end)) as Vasopressin,
      Max((case when me.itemid in ('138', '222062') then 1 else cast('' as numeric) end)) as Vecuronium
    FROM MEDEVENTS me
    GROUP BY me.subject_id, me.charttime
    """

    demo_data = """
        SELECT i.subject_id,
           i.gender,
           i.expire_flg,
           (case when i.dod is null then cast('' as numeric) else i.dod - i.hospital_disch_dt end) as daysFromDischToDeath,
           (case when i.dod is null then cast('' as numeric) else round(strftime('%m',i.dod) - strftime('%m',i.dob)/12,2) end) as ageAtDeath,
           i.icustay_intime,
           i.icustay_outtime,
           i.icustay_los,
           i.icustay_first_careunit, i.icustay_last_careunit,
           i.icustay_expire_flg,
           i.height,
           i.weight_first,
           i.sapsi_first,
           i.sofa_first,
           i.icustay_admit_age,
           d.ethnicity_descr
    FROM ICUSTAY_DETAIL i
    INNER JOIN DEMOGRAPHIC_DETAIL d
    ON i.subject_id = d.subject_id
    """

    ioview = """
      select io.subject_id, io.charttime,
          Sum((Case When io.itemid IN ('55','69','715', '61', '57', '85','473', '405', '428') Then round(io.volume,2) Else cast('' as numeric) End)) AS UrineOut,
          Sum((Case When io.itemid IN ('144','172', '398') Then round(io.volume,2) Else cast('' as numeric) End)) AS InputRBCs,
          Sum((Case When io.itemid IN ('179', '224','3955', '163', '319', '221') Then round(io.volume,2) Else cast('' as numeric) End)) AS InputOtherBlood
      from IOEVENTS io
      GROUP BY io.subject_id,io.charttime
    """

    fullview_io = """
        select subject_id,charttime,

        cast('' as numeric) AS GCS,
        cast('' as numeric) AS Weight,
        cast('' as numeric) AS AdmitWt,
        cast('' as numeric) AS Height,

        cast('' as numeric) AS NBPSys,
        cast('' as numeric) AS NBPDias,
        cast('' as numeric) AS NBPMean,
        cast('' as numeric) AS NBP,
        cast('' as numeric) AS SBP,
        cast('' as numeric) AS DBP,
        cast('' as numeric) AS MAP,
        cast('' as numeric) AS HR,
        cast('' as numeric) AS RESP,
        cast('' as numeric) AS SpO2,
        cast('' as numeric) AS CVP,
        cast('' as numeric) AS PAPMean,
        cast('' as numeric) AS PAPSd,
        cast('' as numeric) AS CrdIndx,
        cast('' as numeric) AS SVR,
        cast('' as numeric) AS COtd,
        cast('' as numeric) AS COfck,
        cast('' as numeric) AS PCWP,
        cast('' as numeric) AS PVR,

        cast('' as numeric) AS Na,
        cast('' as numeric) AS K,
        cast('' as numeric) AS Cl,
        cast('' as numeric) AS CO2,
        cast('' as numeric) AS Glucose,
        cast('' as numeric) AS BUN,
        cast('' as numeric) AS Creatinine,
        cast('' as numeric) AS  Mg,
        cast('' as numeric) AS AST,
        cast('' as numeric) AS ALT,
        cast('' as numeric) AS Ca,
        cast('' as numeric) AS IonCa,
        cast('' as numeric) AS TBili,
        cast('' as numeric) AS DBili,
        cast('' as numeric) AS TProtein,
        cast('' as numeric) AS Albumin,
        cast('' as numeric) AS Lactate,
        cast('' as numeric) AS Troponin,

        cast('' as numeric) AS HCT,
        cast('' as numeric) AS Hg,
        cast('' as numeric) AS Platelets,
        cast('' as numeric) AS INR,
        cast('' as numeric) AS PT,
        cast('' as numeric) AS PTT,
        cast('' as numeric) AS WBC,
        cast('' as numeric) AS RBC,
        cast('' as numeric) AS TEMP,

        cast('' as numeric) AS ArtBE,
        cast('' as numeric) AS ArtCO2,
        cast('' as numeric) AS ArtPaCO2,
        cast('' as numeric) AS ArtPaO2,
        cast('' as numeric) AS ArtpH,

        cast('' as numeric) AS FiO2Set,
        cast('' as numeric) AS PEEPSet,
        cast('' as numeric) AS RespTot,
        cast('' as numeric) AS RespSet,
        cast('' as numeric) AS RespSpon,
        cast('' as numeric) AS PIP,
        cast('' as numeric) AS PlateauPres,
        cast('' as numeric) AS TidVolObs,
        cast('' as numeric) AS TidVolSet,
        cast('' as numeric) AS TidVolSpon,
        cast('' as numeric) AS SaO2,

        '' AS Heart_Rhythm,
        '' AS Ectopy_Type,
        '' AS Ectopy_Freq,
        '' AS Code_Status,
        '' AS FallRisk,
        '' AS Orientation,
        '' AS RikerSAS,
        '' AS Vent,
        '' AS VentMode,
        '' AS Pacemaker,
        '' AS Trach,
        '' AS SkinColor,
        '' AS SkinIntegrity,
        '' AS IABP_Setting,
        '' AS ServiceType,

        urineout,
        inputrbcs, inputotherblood,

        cast('' as numeric) AS Aggrestat,
        cast('' as numeric) AS Aminophylline,
        cast('' as numeric) AS Amiodarone,
        cast('' as numeric) AS Amrinone,
        cast('' as numeric) AS Argatroban,
        cast('' as numeric) AS Ativan,
        cast('' as numeric) AS Atracurium,
        cast('' as numeric) AS Bivalirudin,
        cast('' as numeric) AS Cistracurium,
        cast('' as numeric) AS Dilaudid,
        cast('' as numeric) AS Diltiazem,
        cast('' as numeric) AS Dobutamine,
        cast('' as numeric) AS Dopamine,
        cast('' as numeric) AS Doxacurium,
        cast('' as numeric) AS Epinephrine,
        cast('' as numeric) AS EpinephrineK,
        cast('' as numeric) AS Esmolol,
        cast('' as numeric) AS FentanylConc,
        cast('' as numeric) AS Fentanyl,
        cast('' as numeric) AS Heparin,
        cast('' as numeric) AS Insulin,
        cast('' as numeric) AS Integrelin,
        cast('' as numeric) AS Ketamine,
        cast('' as numeric) AS Labetolol,
        cast('' as numeric) AS Lasix,
        cast('' as numeric) AS Lepirudin,
        cast('' as numeric) AS Levophed,
        cast('' as numeric) AS LevophedK,
        cast('' as numeric) AS Lidocaine,
        cast('' as numeric) AS Midazolam,
        cast('' as numeric) AS Miltinone,
        cast('' as numeric) AS MorphineSolfate,
        cast('' as numeric) AS Narcan,
        cast('' as numeric) AS Natrecor,
        cast('' as numeric) AS Neosynephrine,
        cast('' as numeric) AS NeosynephrineK,
        cast('' as numeric) AS Nicardipine,
        cast('' as numeric) AS Nitroglycerine,
        cast('' as numeric) AS NitroglycerineK,
        cast('' as numeric) AS Nitroprusside,
        cast('' as numeric) AS Pancuronium,
        cast('' as numeric) AS Pentobarbitol,
        cast('' as numeric) AS Precedex,
        cast('' as numeric) AS Procainamide,
        cast('' as numeric) AS Propofol,
        cast('' as numeric) AS Reopro,
        cast('' as numeric) AS Sandostatin,
        cast('' as numeric) AS TPA,
        cast('' as numeric) AS Vasopressin,
        cast('' as numeric) AS Vecuronium

      from IOVIEW
    """
    fullview_chart =  """
      select subject_id,charttime,GCS,Weight,AdmitWt, Height,
        NBPSys,NBPDias,NBPMean,NBP,SBP,DBP,MAP,HR,RESP,SpO2,CVP,PAPMean,PAPSd,CrdIndx,SVR,COtd,COfck,PCWP,PVR,

        Na,K,Cl,CO2,Glucose,BUN,Creatinine, Mg,AST,ALT,Ca,IonCa,TBili,DBili,TProtein,Albumin,Lactate,Troponin,

        HCT,Hg,Platelets,INR,PT,PTT,WBC,RBC,TEMP,

        ArtBE,ArtCO2,ArtPaCO2,ArtPaO2,ArtpH,

        FiO2Set,PEEPSet,RespTot,RespSet,RespSpon,PIP,PlateauPres,TidVolObs,TidVolSet,TidVolSpon,SaO2,

        Heart_Rhythm,Ectopy_Type,Ectopy_Freq,Code_Status,FallRisk,Orientation,
        RikerSAS,Vent,VentMode,Pacemaker,Trach,SkinColor,SkinIntegrity,IABP_Setting,ServiceType,

        cast('' as numeric) AS urineout,
        cast('' as numeric) AS inputrbcs,
        cast('' as numeric) AS inputotherblood,

        cast('' as numeric) AS Aggrestat,
        cast('' as numeric) AS Aminophylline,
        cast('' as numeric) AS Amiodarone,
        cast('' as numeric) AS Amrinone,
        cast('' as numeric) AS Argatroban,
        cast('' as numeric) AS Ativan,
        cast('' as numeric) AS Atracurium,
        cast('' as numeric) AS Bivalirudin,
        cast('' as numeric) AS Cistracurium,
        cast('' as numeric) AS Dilaudid,
        cast('' as numeric) AS Diltiazem,
        cast('' as numeric) AS Dobutamine,
        cast('' as numeric) AS Dopamine,
        cast('' as numeric) AS Doxacurium,
        cast('' as numeric) AS Epinephrine,
        cast('' as numeric) AS EpinephrineK,
        cast('' as numeric) AS Esmolol,
        cast('' as numeric) AS FentanylConc,
        cast('' as numeric) AS Fentanyl,
        cast('' as numeric) AS Heparin,
        cast('' as numeric) AS Insulin,
        cast('' as numeric) AS Integrelin,
        cast('' as numeric) AS Ketamine,
        cast('' as numeric) AS Labetolol,
        cast('' as numeric) AS Lasix,
        cast('' as numeric) AS Lepirudin,
        cast('' as numeric) AS Levophed,
        cast('' as numeric) AS LevophedK,
        cast('' as numeric) AS Lidocaine,
        cast('' as numeric) AS Midazolam,
        cast('' as numeric) AS Miltinone,
        cast('' as numeric) AS MorphineSolfate,
        cast('' as numeric) AS Narcan,
        cast('' as numeric) AS Natrecor,
        cast('' as numeric) AS Neosynephrine,
        cast('' as numeric) AS NeosynephrineK,
        cast('' as numeric) AS Nicardipine,
        cast('' as numeric) AS Nitroglycerine,
        cast('' as numeric) AS NitroglycerineK,
        cast('' as numeric) AS Nitroprusside,
        cast('' as numeric) AS Pancuronium,
        cast('' as numeric) AS Pentobarbitol,
        cast('' as numeric) AS Precedex,
        cast('' as numeric) AS Procainamide,
        cast('' as numeric) AS Propofol,
        cast('' as numeric) AS Reopro,
        cast('' as numeric) AS Sandostatin,
        cast('' as numeric) AS TPA,
        cast('' as numeric) AS Vasopressin,
        cast('' as numeric) AS Vecuronium

        from CHARTVIEW
    """

    fullview_med = """
        select subject_id,charttime,

        cast('' as numeric) AS GCS,
        cast('' as numeric) AS Weight,
        cast('' as numeric) AS AdmitWt,
        cast('' as numeric) AS Height,

        cast('' as numeric) AS NBPSys,
        cast('' as numeric) AS NBPDias,
        cast('' as numeric) AS NBPMean,
        cast('' as numeric) AS NBP,
        cast('' as numeric) AS SBP,
        cast('' as numeric) AS DBP,
        cast('' as numeric) AS MAP,
        cast('' as numeric) AS HR,
        cast('' as numeric) AS RESP,
        cast('' as numeric) AS SpO2,
        cast('' as numeric) AS CVP,
        cast('' as numeric) AS PAPMean,
        cast('' as numeric) AS PAPSd,
        cast('' as numeric) AS CrdIndx,
        cast('' as numeric) AS SVR,
        cast('' as numeric) AS COtd,
        cast('' as numeric) AS COfck,
        cast('' as numeric) AS PCWP,
        cast('' as numeric) AS PVR,

        cast('' as numeric) AS Na,
        cast('' as numeric) AS K,
        cast('' as numeric) AS Cl,
        cast('' as numeric) AS CO2,
        cast('' as numeric) AS Glucose,
        cast('' as numeric) AS BUN,
        cast('' as numeric) AS Creatinine,
        cast('' as numeric) AS Mg,
        cast('' as numeric) AS AST,
        cast('' as numeric) AS ALT,
        cast('' as numeric) AS Ca,
        cast('' as numeric) AS IonCa,
        cast('' as numeric) AS TBili,
        cast('' as numeric) AS DBili,
        cast('' as numeric) AS TProtein,
        cast('' as numeric) AS Albumin,
        cast('' as numeric) AS Lactate,
        cast('' as numeric) AS Troponin,

        cast('' as numeric) AS HCT,
        cast('' as numeric) AS Hg,
        cast('' as numeric) AS Platelets,
        cast('' as numeric) AS INR,
        cast('' as numeric) AS PT,
        cast('' as numeric) AS PTT,
        cast('' as numeric) AS WBC,
        cast('' as numeric) AS RBC,
        cast('' as numeric) AS TEMP,

        cast('' as numeric) AS ArtBE,
        cast('' as numeric) AS ArtCO2,
        cast('' as numeric) AS ArtPaCO2,
        cast('' as numeric) AS ArtPaO2,
        cast('' as numeric) AS ArtpH,

        cast('' as numeric) AS FiO2Set,
        cast('' as numeric) AS PEEPSet,
        cast('' as numeric) AS RespTot,
        cast('' as numeric) AS RespSet,
        cast('' as numeric) AS RespSpon,
        cast('' as numeric) AS PIP,
        cast('' as numeric) AS PlateauPres,
        cast('' as numeric) AS TidVolObs,
        cast('' as numeric) AS TidVolSet,
        cast('' as numeric) AS TidVolSpon,
        cast('' as numeric) AS SaO2,

        '' AS Heart_Rhythm,
        '' AS Ectopy_Type,
        '' AS Ectopy_Freq,
        '' AS Code_Status,
        '' AS FallRisk,
        '' AS Orientation,
        '' AS RikerSAS,
        '' AS Vent,
        '' AS VentMode,
        '' AS Pacemaker,
        '' AS Trach,
        '' AS SkinColor,
        '' AS SkinIntegrity,
        '' AS IABP_Setting,
        '' AS ServiceType,

        cast('' as numeric) AS urineout,
        cast('' as numeric) AS inputrbcs,
        cast('' as numeric) AS inputotherblood,

        Aggrestat,Aminophylline,Amiodarone,Amrinone,Argatroban,Ativan,Atracurium,
        Bivalirudin,Cistracurium,Dilaudid,Diltiazem,Dobutamine,Dopamine,Doxacurium,
        Epinephrine,EpinephrineK,Esmolol,FentanylConc,Fentanyl,Heparin,Insulin,Integrelin,
        Ketamine,Labetolol,Lasix,Lepirudin,Levophed,LevophedK,Lidocaine,Midazolam,
        Miltinone,MorphineSolfate,Narcan,Natrecor,Neosynephrine,NeosynephrineK,
        Nicardipine,Nitroglycerine,NitroglycerineK,Nitroprusside,Pancuronium,
        Pentobarbitol,Precedex,Procainamide,Propofol,Reopro,Sandostatin,TPA,Vasopressin,
        Vecuronium

        from MED_VIEW
    """
    merged_fullview = """
        select *
        from FULLVIEW_IO

        UNION ALL

        select *
        from FULLVIEW_CHART

        UNION ALL

        select *
        from FULLVIEW_MED
    """

    combined_full_view = """
      select subject_id, charttime, max(GCS) as GCS, max(Weight) as Weight, max(AdmitWt) as AdmitWt, max(Height) as Height,

        max(NBPSys) as NBPSys,
        max(NBPDias) as NBPDias,
        max(NBPMean) as NBPMean,
        max(NBP) as NBP,
        max(SBP) as SBP,
        max(DBP) as DBP,
        max(MAP) as MAP,
        max(HR) as HR,
        max(RESP) as RESP,
        max(SpO2) as SpO2,
        max(CVP) as CVP,
        max(PAPMean) as PAPMean,
        max(PAPSd) as PAPSd,
        max(CrdIndx) as CrdIndx,
        max(SVR) as SVR,
        max(COtd) as COtd,
        max(COfck) as COfck,
        max(PCWP) as PCWP,
        max(PVR) as PVR,

        max(Na) as Na,
        max(K) as K,
        max(Cl) as Cl,
        max(CO2) as CO2,
        max(Glucose) as Glucose,
        max(BUN) as BUN,
        max(Creatinine) as Creatinine,
        max(Mg) as Mg,
        max(AST) as AST,
        max(ALT) as ALT,
        max(Ca) as Ca,
        max(IonCa) as IonCa,
        max(TBili) as TBili,
        max(DBili) as DBili,
        max(TProtein) as TProtein,
        max(Albumin) as Albumin,
        max(Lactate) as Lactate,
        max(Troponin) as Troponin,

        max(HCT) as HCT,
        max(Hg) as Hg,
        max(Platelets) as Platelets,
        max(INR) as INR,
        max(PT) as PT,
        max(PTT) as PTT,
        max(WBC) as WBC,
        max(RBC) as RBC,
        max(TEMP) as TEMP,

        max(ArtBE) as ArtBE,
        max(ArtCO2) as ArtCO2,
        max(ArtPaCO2) as ArtPaCO2,
        max(ArtPaO2) as ArtPaO2,
        max(ArtpH) as ArtpH,

        max(FiO2Set) as FiO2Set,
        max(PEEPSet) as PEEPSet,
        max(RespTot) as RespTot,
        max(RespSet) as RespSet,
        max(RespSpon) as RespSpon,
        max(PIP) as PIP,
        max(PlateauPres) as PlateauPres,
        max(TidVolObs) as TidVolObs,
        max(TidVolSet) as TidVolSet,
        max(TidVolSpon) as TidVolSpon,
        max(SaO2) as SaO2,

        max(Heart_Rhythm) as Heart_Rhythm,
        max(Ectopy_Type) as Ectopy_Type,
        max(Ectopy_Freq) as Ectopy_Freq,
        max(Code_Status) as Code_Status,
        max(FallRisk) as FallRisk,
        max(Orientation) as Orientation,
        max(RikerSAS) as RikerSAS,
        max(Vent) as Vent,
        max(VentMode) as VentMode,
        max(Pacemaker) as Pacemaker,
        max(Trach) as Trach,
        max(SkinColor) as SkinColor,
        max(SkinIntegrity) as SkinIntegrity,
        max(IABP_Setting) as IABP_Setting,
        max(ServiceType) as ServiceType,

        max(urineout) as urineout,
        max(inputrbcs) as inputrbcs,
        max(inputotherblood) as inputotherblood,

        max(Aggrestat) as Aggrestat,
        max(Aminophylline) as Aminophylline,
        max(Amiodarone) as Amiodarone,
        max(Amrinone) as Amrinone,
        max(Argatroban) as Argatroban,
        max(Ativan) as Ativan,
        max(Atracurium) as Atracurium,
        max(Bivalirudin) as Bivalirudin,
        max(Cistracurium) as Cistracurium,
        max(Dilaudid) as Dilaudid,
        max(Diltiazem) as Diltiazem,
        max(Dobutamine) as Dobutamine,
        max(Dopamine) as Dopamine,
        max(Doxacurium) as Doxacurium,
        max(Epinephrine) as Epinephrine,
        max(EpinephrineK) as EpinephrineK,
        max(Esmolol) as Esmolol,
        max(FentanylConc) as FentanylConc,
        max(Fentanyl) as Fentanyl,
        max(Heparin) as Heparin,
        max(Insulin) as Insulin,
        max(Integrelin) as Integrelin,
        max(Ketamine) as Ketamine,
        max(Labetolol) as Labetolol,
        max(Lasix) as Lasix,
        max(Lepirudin) as Lepirudin,
        max(Levophed) as Levophed,
        max(LevophedK) as LevophedK,
        max(Lidocaine) as Lidocaine,
        max(Midazolam) as Midazolam,
        max(Miltinone) as Miltinone,
        max(MorphineSolfate) as MorphineSolfate,
        max(Narcan) as Narcan,
        max(Natrecor) as Natrecor,
        max(Neosynephrine) as Neosynephrine,
        max(NeosynephrineK) as NeosynephrineK,
        max(Nicardipine) as Nicardipine,
        max(Nitroglycerine) as Nitroglycerine,
        max(NitroglycerineK) as NitroglycerineK,
        max(Nitroprusside) as Nitroprusside,
        max(Pancuronium) as Pancuronium,
        max(Pentobarbitol) as Pentobarbitol,
        max(Precedex) as Precedex,
        max(Procainamide) as Procainamide,
        max(Propofol) as Propofol,
        max(Reopro) as Reopro,
        max(Sandostatin) as Sandostatin,
        max(TPA) as TPA,
        max(Vasopressin) as Vasopressin,
        max(Vecuronium) as Vecuronium

      from FULLVIEW

      group by subject_id, charttime

    """

    combined_data = """
        SELECT f.*,
            d.gender,
            d.icustay_admit_age,
            d.icustay_expire_flg,
            d.daysFromDischToDeath,
            d.ageAtDeath,
            d.icustay_intime,
            d.icustay_outtime,
            d.icustay_los,
            d.icustay_first_careunit,
            d.icustay_last_careunit,
            d.weight_first,
            d.ethnicity_descr
      FROM  COMBINED_FULLVIEW f
      INNER JOIN DEMO_DATA d
        ON f.subject_id = d.subject_id
      WHERE f.charttime >= d.icustay_intime and f.charttime <= d.icustay_outtime
    """

    agefilter = """
      SELECT c.*
      FROM COMBINED_DATA c
      INNER JOIN AGEDATA a
      ON c.subject_id = a.subject_id
    """

    partition_db['AGEDATA'] = sqldf(agedata, partition_db)
    partition_db['CHARTVIEW'] = sqldf(chartview, partition_db)
    partition_db['CHARTEVENTS'] = []
    partition_db['MED_VIEW'] = sqldf(med_view, partition_db)
    partition_db['MEDEVENTS'] = []
    partition_db['DEMO_DATA'] = sqldf(demo_data, partition_db)
    partition_db['FROM ICUSTAY_DETAIL'] = []
    partition_db['DEMOGRAPHIC_DETAIL'] = []
    partition_db['IOVIEW'] = sqldf(ioview, partition_db)
    partition_db['IOEVENTS'] = []
    partition_db['FULLVIEW_IO'] = sqldf(fullview_io, partition_db)
    partition_db['IOVIEW'] = []
    partition_db['FULLVIEW_CHART'] = sqldf(fullview_chart, partition_db)
    partition_db['CHARTVIEW'] = []
    partition_db['FULLVIEW_MED'] = sqldf(fullview_med, partition_db)
    partition_db['MEDVIEW'] = []
    partition_db['FULLVIEW'] = sqldf(merged_fullview, partition_db)
    partition_db['FULLVIEW_IO'] = []
    partition_db['FULLVIEW_CHART'] = []
    partition_db['FULLVIEW_MED'] = []
    partition_db['COMBINED_FULLVIEW'] = sqldf(combined_full_view, partition_db)
    partition_db['FULLVIEW'] = []
    partition_db['COMBINED_DATA'] = sqldf(combined_data, partition_db)
    partition_db['COMBINEDVIEW'] = []
    partition_db['AGE_FILTERED_DATA'] = sqldf(agefilter, partition_db)
    partition_db['COMBINED_DATA'] = []
    partition_db['AGE_FILTERED_DATA'].to_csv(path_syntax + 'outcome_pickle_' + str(count) + '.csv', index=False)
    count += 1