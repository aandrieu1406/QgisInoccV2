-- FUNCTION: public.__ind_niv1(text, text, text)

-- DROP FUNCTION IF EXISTS public.__ind_niv1(text, text, text);

CREATE OR REPLACE FUNCTION public.__ind_niv1(
	an1 text,
	an2 text,
	dat text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    --VOLATILE PARALLEL UNSAFE
AS $BODY$

begin

EXECUTE 'DROP TABLE IF EXISTS public.ind_niv1_'||dat||' CASCADE';
EXECUTE 'CREATE TABLE public.ind_niv1_'||dat||' AS
WITH temp AS
(SELECT 
	*,
	CASE
		WHEN (codtypbien LIKE ''11%'' AND sbati != 0) OR (codtypbien LIKE ''12%'' AND sbati != 0) THEN ROUND(valeurfonc/sbati*1.0,2)
	END AS pxm2,
	CASE
		WHEN codtypbien LIKE ''111%'' THEN ''Maison''
		WHEN codtypbien LIKE ''121%'' THEN ''Appartement''
	END AS type_bien,
	/* Création colonne taille de bien en fonction de la surface (maison) et de l''identifiant (appartements) */
	CASE
		WHEN codtypbien LIKE ''111%'' AND sbatmai < 90 THEN ''Petit'' -- classification des petites maisons
		WHEN codtypbien LIKE ''121_1'' OR codtypbien LIKE ''121_2''  THEN ''Petit'' -- classification des petits appartements (T1 et T2)
		
		WHEN codtypbien LIKE ''111%'' AND sbatmai < 130 THEN ''Moyen'' -- classification des moyennes maisons
		WHEN codtypbien LIKE ''121_3'' OR codtypbien LIKE ''121_4'' THEN ''Moyen'' -- classification des moyens appartements (T3 et T4) 
		
		WHEN codtypbien LIKE ''111%'' AND sbatmai >= 130 THEN ''Grand'' -- classification des grandes maisons
		WHEN codtypbien LIKE ''121_5'' THEN ''Grand'' -- classification des grands appartements (T5+)
		
	END AS taille,
	/* Création nouvelle colonne periodecst avec découpage actualisé  (< 1945 | 1945-1960 | 1961-1974 | 1975-2012 | > 2012) */
	CASE
		WHEN periodecst IN (''< 1914'',''1914-1944'') THEN ''< 1945''
		WHEN periodecst IN (''1975-1989'',''1990-2012'') THEN ''1975-2012''
	ELSE periodecst
	END AS periode_cst
	
FROM dvf.mutation)

SELECT 
    b.periode,
    b.id,
    b.nom,

	-- Bâti (code 1)

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''1%'') as nbtrans_cod1,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''1%'') as valeurfonc_sum_cod1,

	-- Non bâti (code 2)

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''2%'') as nbtrans_cod2,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''2%'') as valeurfonc_sum_cod2,

	-- Ensemble des maisons

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''11%'') as nbtrans_cod11,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''11%'') as valeurfonc_sum_cod11,
	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''111%'') as nbtrans_cod111,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''111%'') as valeurfonc_sum_cod111,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''111%'') as valeurfonc_q25_cod111,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''111%'') as valeurfonc_median_cod111,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''111%'') as valeurfonc_q75_cod111,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''111%'') as pxm2_q25_cod111,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''111%'') as pxm2_median_cod111,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''111%'') as pxm2_q75_cod111,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''111%'') as sbati_sum_cod111,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''111%'') as sbati_median_cod111,

	-- Maisons à usage d_habitation

	---- Neuf/VEFA

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''1111'') as nbtrans_cod1111,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''1111'') as valeurfonc_sum_cod1111,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1111'') as valeurfonc_q25_cod1111,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1111'') as valeurfonc_median_cod1111,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1111'') as valeurfonc_q75_cod1111,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1111'') as pxm2_q25_cod1111,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1111'') as pxm2_median_cod1111,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1111'') as pxm2_q75_cod1111,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''1111'') as sbati_sum_cod1111,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''1111'') as sbati_median_cod1111,

	---- Récent

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''1112'') as nbtrans_cod1112,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''1112'') as valeurfonc_sum_cod1112,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1112'') as valeurfonc_q25_cod1112,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1112'') as valeurfonc_median_cod1112,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1112'') as valeurfonc_q75_cod1112,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1112'') as pxm2_q25_cod1112,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1112'') as pxm2_median_cod1112,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1112'') as pxm2_q75_cod1112,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''1112'') as sbati_sum_cod1112,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''1112'') as sbati_median_cod1112,

	---- Ancien

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''1113'') as nbtrans_cod1113,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''1113'') as valeurfonc_sum_cod1113,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1113'') as valeurfonc_q25_cod1113,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1113'') as valeurfonc_median_cod1113,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1113'') as valeurfonc_q75_cod1113,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1113'') as pxm2_q25_cod1113,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1113'') as pxm2_median_cod1113,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1113'') as pxm2_q75_cod1113,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''1113'') as sbati_sum_cod1113,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''1113'') as sbati_median_cod1113,

	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as nbtrans_mp1,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as valeurfonc_sum_mp1,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as valeurfonc_q25_mp1,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as valeurfonc_median_mp1,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as valeurfonc_q75_mp1,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as pxm2_q25_mp1,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as pxm2_median_mp1,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as pxm2_q75_mp1,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as sbati_sum_mp1,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as sbati_median_mp1
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as nbtrans_mp2,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as valeurfonc_sum_mp2,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q25_mp2,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as valeurfonc_median_mp2,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q75_mp2,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as pxm2_q25_mp2,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as pxm2_median_mp2,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as pxm2_q75_mp2,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as sbati_sum_mp2,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as sbati_median_mp2
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as nbtrans_mp3,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as valeurfonc_sum_mp3,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q25_mp3,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as valeurfonc_median_mp3,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q75_mp3,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as pxm2_q25_mp3,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as pxm2_median_mp3,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as pxm2_q75_mp3,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as sbati_sum_mp3,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as sbati_median_mp3
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as nbtrans_mp4,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as valeurfonc_sum_mp4,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q25_mp4,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as valeurfonc_median_mp4,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q75_mp4,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as pxm2_q25_mp4,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as pxm2_median_mp4,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as pxm2_q75_mp4,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as sbati_sum_mp4,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as sbati_median_mp4
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as nbtrans_mp5,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as valeurfonc_sum_mp5,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q25_mp5,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as valeurfonc_median_mp5,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q75_mp5,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as pxm2_q25_mp5,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as pxm2_median_mp5,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as pxm2_q75_mp5,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as sbati_sum_mp5,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as sbati_median_mp5
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') ) as nbtrans_mpx,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') ) as valeurfonc_sum_mpx,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') ) as valeurfonc_q25_mpx,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') ) as valeurfonc_median_mpx,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') ) as valeurfonc_q75_mpx,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') ) as pxm2_q25_mpx,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') ) as pxm2_median_mpx,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') ) as pxm2_q75_mpx,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') ) as sbati_sum_mpx,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Petit'') ) as sbati_median_mpx
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as nbtrans_mm1,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as valeurfonc_sum_mm1,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as valeurfonc_q25_mm1,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as valeurfonc_median_mm1,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as valeurfonc_q75_mm1,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as pxm2_q25_mm1,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as pxm2_median_mm1,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as pxm2_q75_mm1,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as sbati_sum_mm1,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as sbati_median_mm1
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as nbtrans_mm2,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as valeurfonc_sum_mm2,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q25_mm2,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as valeurfonc_median_mm2,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q75_mm2,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as pxm2_q25_mm2,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as pxm2_median_mm2,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as pxm2_q75_mm2,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as sbati_sum_mm2,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as sbati_median_mm2
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as nbtrans_mm3,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as valeurfonc_sum_mm3,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q25_mm3,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as valeurfonc_median_mm3,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q75_mm3,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as pxm2_q25_mm3,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as pxm2_median_mm3,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as pxm2_q75_mm3,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as sbati_sum_mm3,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as sbati_median_mm3
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as nbtrans_mm4,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as valeurfonc_sum_mm4,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q25_mm4,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as valeurfonc_median_mm4,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q75_mm4,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as pxm2_q25_mm4,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as pxm2_median_mm4,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as pxm2_q75_mm4,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as sbati_sum_mm4,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as sbati_median_mm4
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as nbtrans_mm5,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as valeurfonc_sum_mm5,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q25_mm5,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as valeurfonc_median_mm5,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q75_mm5,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as pxm2_q25_mm5,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as pxm2_median_mm5,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as pxm2_q75_mm5,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as sbati_sum_mm5,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as sbati_median_mm5
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') ) as nbtrans_mmx,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') ) as valeurfonc_sum_mmx,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') ) as valeurfonc_q25_mmx,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') ) as valeurfonc_median_mmx,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') ) as valeurfonc_q75_mmx,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') ) as pxm2_q25_mmx,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') ) as pxm2_median_mmx,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') ) as pxm2_q75_mmx,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') ) as sbati_sum_mmx,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Moyen'') ) as sbati_median_mmx
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as nbtrans_mg1,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as valeurfonc_sum_mg1,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as valeurfonc_q25_mg1,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as valeurfonc_median_mg1,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as valeurfonc_q75_mg1,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as pxm2_q25_mg1,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as pxm2_median_mg1,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as pxm2_q75_mg1,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as sbati_sum_mg1,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as sbati_median_mg1
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as nbtrans_mg2,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as valeurfonc_sum_mg2,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q25_mg2,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as valeurfonc_median_mg2,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q75_mg2,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as pxm2_q25_mg2,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as pxm2_median_mg2,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as pxm2_q75_mg2,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as sbati_sum_mg2,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as sbati_median_mg2
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as nbtrans_mg3,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as valeurfonc_sum_mg3,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q25_mg3,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as valeurfonc_median_mg3,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q75_mg3,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as pxm2_q25_mg3,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as pxm2_median_mg3,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as pxm2_q75_mg3,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as sbati_sum_mg3,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as sbati_median_mg3
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as nbtrans_mg4,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as valeurfonc_sum_mg4,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q25_mg4,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as valeurfonc_median_mg4,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q75_mg4,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as pxm2_q25_mg4,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as pxm2_median_mg4,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as pxm2_q75_mg4,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as sbati_sum_mg4,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as sbati_median_mg4
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as nbtrans_mg5,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as valeurfonc_sum_mg5,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q25_mg5,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as valeurfonc_median_mg5,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q75_mg5,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as pxm2_q25_mg5,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as pxm2_median_mg5,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as pxm2_q75_mg5,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as sbati_sum_mg5,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as sbati_median_mg5
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') ) as nbtrans_mgx,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') ) as valeurfonc_sum_mgx,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') ) as valeurfonc_q25_mgx,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') ) as valeurfonc_median_mgx,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') ) as valeurfonc_q75_mgx,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') ) as pxm2_q25_mgx,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') ) as pxm2_median_mgx,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') ) as pxm2_q75_mgx,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') ) as sbati_sum_mgx,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'') AND taille IN (''Grand'') ) as sbati_median_mgx
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''< 1945'')) as nbtrans_mx1,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''< 1945'')) as valeurfonc_sum_mx1,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''< 1945'')) as valeurfonc_q25_mx1,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''< 1945'')) as valeurfonc_median_mx1,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''< 1945'')) as valeurfonc_q75_mx1,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''< 1945'')) as pxm2_q25_mx1,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''< 1945'')) as pxm2_median_mx1,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''< 1945'')) as pxm2_q75_mx1,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''< 1945'')) as sbati_sum_mx1,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''< 1945'')) as sbati_median_mx1
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1945-1960'')) as nbtrans_mx2,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1945-1960'')) as valeurfonc_sum_mx2,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1945-1960'')) as valeurfonc_q25_mx2,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1945-1960'')) as valeurfonc_median_mx2,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1945-1960'')) as valeurfonc_q75_mx2,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1945-1960'')) as pxm2_q25_mx2,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1945-1960'')) as pxm2_median_mx2,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1945-1960'')) as pxm2_q75_mx2,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1945-1960'')) as sbati_sum_mx2,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1945-1960'')) as sbati_median_mx2
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1961-1974'')) as nbtrans_mx3,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1961-1974'')) as valeurfonc_sum_mx3,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1961-1974'')) as valeurfonc_q25_mx3,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1961-1974'')) as valeurfonc_median_mx3,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1961-1974'')) as valeurfonc_q75_mx3,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1961-1974'')) as pxm2_q25_mx3,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1961-1974'')) as pxm2_median_mx3,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1961-1974'')) as pxm2_q75_mx3,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1961-1974'')) as sbati_sum_mx3,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1961-1974'')) as sbati_median_mx3
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1975-2012'')) as nbtrans_mx4,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1975-2012'')) as valeurfonc_sum_mx4,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1975-2012'')) as valeurfonc_q25_mx4,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1975-2012'')) as valeurfonc_median_mx4,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1975-2012'')) as valeurfonc_q75_mx4,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1975-2012'')) as pxm2_q25_mx4,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1975-2012'')) as pxm2_median_mx4,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1975-2012'')) as pxm2_q75_mx4,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1975-2012'')) as sbati_sum_mx4,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''1975-2012'')) as sbati_median_mx4
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''>= 2013'')) as nbtrans_mx5,   
	sum(valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''>= 2013'')) as valeurfonc_sum_mx5,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''>= 2013'')) as valeurfonc_q25_mx5,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''>= 2013'')) as valeurfonc_median_mx5,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''>= 2013'')) as valeurfonc_q75_mx5,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''>= 2013'')) as pxm2_q25_mx5,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''>= 2013'')) as pxm2_median_mx5,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''>= 2013'')) as pxm2_q75_mx5,
	sum(sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''>= 2013'')) as sbati_sum_mx5,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien in (''1110'',''1111'',''1112'',''1113'')  AND periode_cst IN (''>= 2013'')) as sbati_median_mx5
	,
	-- Maisons à usage professionel (code 1114)

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''1114'') as nbtrans_cod1114,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''1114'') as valeurfonc_sum_cod1114,

	-- Ensemble des appartements

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''12%'') as nbtrans_cod12,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''12%'') as valeurfonc_sum_cod12,
	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''121%'') as nbtrans_cod121,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''121%'') as valeurfonc_sum_cod121,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121%'') as valeurfonc_q25_cod121,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121%'') as valeurfonc_median_cod121,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121%'') as valeurfonc_q75_cod121,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121%'') as pxm2_q25_cod121,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121%'') as pxm2_median_cod121,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121%'') as pxm2_q75_cod121,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''121%'') as sbati_sum_cod121,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''121%'') as sbati_median_cod121,

	-- Appartements à usage d_habitation

	---- Neuf/VEFA

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''1211%'') as nbtrans_cod1211,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''1211%'') as valeurfonc_sum_cod1211,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1211%'') as valeurfonc_q25_cod1211,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1211%'') as valeurfonc_median_cod1211,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1211%'') as valeurfonc_q75_cod1211,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1211%'') as pxm2_q25_cod1211,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1211%'') as pxm2_median_cod1211,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1211%'') as pxm2_q75_cod1211,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''1211%'') as sbati_sum_cod1211,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''1211%'') as sbati_median_cod1211,

	---- Récent

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''1212%'') as nbtrans_cod1212,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''1212%'') as valeurfonc_sum_cod1212,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1212%'') as valeurfonc_q25_cod1212,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1212%'') as valeurfonc_median_cod1212,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1212%'') as valeurfonc_q75_cod1212,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1212%'') as pxm2_q25_cod1212,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1212%'') as pxm2_median_cod1212,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1212%'') as pxm2_q75_cod1212,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''1212%'') as sbati_sum_cod1212,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''1212%'') as sbati_median_cod1212,

	---- Ancien

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''1213%'') as nbtrans_cod1213,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''1213%'') as valeurfonc_sum_cod1213,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1213%'') as valeurfonc_q25_cod1213,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1213%'') as valeurfonc_median_cod1213,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''1213%'') as valeurfonc_q75_cod1213,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1213%'') as pxm2_q25_cod1213,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1213%'') as pxm2_median_cod1213,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''1213%'') as pxm2_q75_cod1213,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''1213%'') as sbati_sum_cod1213,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''1213%'') as sbati_median_cod1213,

	---- T1

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''121_1'') as nbtrans_cod121x1,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''121_1'') as valeurfonc_sum_cod121x1,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_1'') as valeurfonc_q25_cod121x1,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_1'') as valeurfonc_median_cod121x1,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_1'') as valeurfonc_q75_cod121x1,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_1'') as pxm2_q25_cod121x1,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_1'') as pxm2_median_cod121x1,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_1'') as pxm2_q75_cod121x1,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''121_1'') as sbati_sum_cod121x1,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''121_1'') as sbati_median_cod121x1,

	---- T2

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''121_2'') as nbtrans_cod121x2,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''121_2'') as valeurfonc_sum_cod121x2,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_2'') as valeurfonc_q25_cod121x2,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_2'') as valeurfonc_median_cod121x2,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_2'') as valeurfonc_q75_cod121x2,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_2'') as pxm2_q25_cod121x2,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_2'') as pxm2_median_cod121x2,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_2'') as pxm2_q75_cod121x2,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''121_2'') as sbati_sum_cod121x2,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''121_2'') as sbati_median_cod121x2,

	---- T3

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''121_3'') as nbtrans_cod121x3,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''121_3'') as valeurfonc_sum_cod121x3,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_3'') as valeurfonc_q25_cod121x3,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_3'') as valeurfonc_median_cod121x3,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_3'') as valeurfonc_q75_cod121x3,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_3'') as pxm2_q25_cod121x3,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_3'') as pxm2_median_cod121x3,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_3'') as pxm2_q75_cod121x3,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''121_3'') as sbati_sum_cod121x3,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''121_3'') as sbati_median_cod121x3,

	---- T4

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''121_4'') as nbtrans_cod121x4,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''121_4'') as valeurfonc_sum_cod121x4,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_4'') as valeurfonc_q25_cod121x4,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_4'') as valeurfonc_median_cod121x4,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_4'') as valeurfonc_q75_cod121x4,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_4'') as pxm2_q25_cod121x4,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_4'') as pxm2_median_cod121x4,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_4'') as pxm2_q75_cod121x4,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''121_4'') as sbati_sum_cod121x4,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''121_4'') as sbati_median_cod121x4,

	---- T5+

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''121_5'') as nbtrans_cod121x5,   
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''121_5'') as valeurfonc_sum_cod121x5,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_5'') as valeurfonc_q25_cod121x5,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_5'') as valeurfonc_median_cod121x5,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien LIKE ''121_5'') as valeurfonc_q75_cod121x5,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_5'') as pxm2_q25_cod121x5,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_5'') as pxm2_median_cod121x5,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien LIKE ''121_5'') as pxm2_q75_cod121x5,
	sum(sbati) FILTER (WHERE codtypbien LIKE ''121_5'') as sbati_sum_cod121x5,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien LIKE ''121_5'') as sbati_median_cod121x5,

	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as nbtrans_ap1,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as valeurfonc_sum_ap1,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as valeurfonc_q25_ap1,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as valeurfonc_median_ap1,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as valeurfonc_q75_ap1,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as pxm2_q25_ap1,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as pxm2_median_ap1,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as pxm2_q75_ap1,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as sbati_sum_ap1,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''< 1945'')) as sbati_median_ap1
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as nbtrans_ap2,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as valeurfonc_sum_ap2,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q25_ap2,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as valeurfonc_median_ap2,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q75_ap2,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as pxm2_q25_ap2,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as pxm2_median_ap2,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as pxm2_q75_ap2,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as sbati_sum_ap2,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1945-1960'')) as sbati_median_ap2
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as nbtrans_ap3,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as valeurfonc_sum_ap3,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q25_ap3,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as valeurfonc_median_ap3,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q75_ap3,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as pxm2_q25_ap3,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as pxm2_median_ap3,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as pxm2_q75_ap3,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as sbati_sum_ap3,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1961-1974'')) as sbati_median_ap3
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as nbtrans_ap4,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as valeurfonc_sum_ap4,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q25_ap4,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as valeurfonc_median_ap4,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q75_ap4,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as pxm2_q25_ap4,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as pxm2_median_ap4,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as pxm2_q75_ap4,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as sbati_sum_ap4,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''1975-2012'')) as sbati_median_ap4
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as nbtrans_ap5,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as valeurfonc_sum_ap5,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q25_ap5,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as valeurfonc_median_ap5,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q75_ap5,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as pxm2_q25_ap5,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as pxm2_median_ap5,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as pxm2_q75_ap5,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as sbati_sum_ap5,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') AND periode_cst IN (''>= 2013'')) as sbati_median_ap5
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') ) as nbtrans_apx,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') ) as valeurfonc_sum_apx,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') ) as valeurfonc_q25_apx,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') ) as valeurfonc_median_apx,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') ) as valeurfonc_q75_apx,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') ) as pxm2_q25_apx,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') ) as pxm2_median_apx,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') ) as pxm2_q75_apx,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') ) as sbati_sum_apx,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Petit'') ) as sbati_median_apx
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as nbtrans_am1,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as valeurfonc_sum_am1,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as valeurfonc_q25_am1,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as valeurfonc_median_am1,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as valeurfonc_q75_am1,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as pxm2_q25_am1,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as pxm2_median_am1,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as pxm2_q75_am1,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as sbati_sum_am1,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''< 1945'')) as sbati_median_am1
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as nbtrans_am2,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as valeurfonc_sum_am2,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q25_am2,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as valeurfonc_median_am2,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q75_am2,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as pxm2_q25_am2,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as pxm2_median_am2,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as pxm2_q75_am2,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as sbati_sum_am2,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1945-1960'')) as sbati_median_am2
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as nbtrans_am3,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as valeurfonc_sum_am3,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q25_am3,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as valeurfonc_median_am3,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q75_am3,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as pxm2_q25_am3,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as pxm2_median_am3,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as pxm2_q75_am3,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as sbati_sum_am3,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1961-1974'')) as sbati_median_am3
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as nbtrans_am4,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as valeurfonc_sum_am4,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q25_am4,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as valeurfonc_median_am4,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q75_am4,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as pxm2_q25_am4,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as pxm2_median_am4,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as pxm2_q75_am4,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as sbati_sum_am4,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''1975-2012'')) as sbati_median_am4
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as nbtrans_am5,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as valeurfonc_sum_am5,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q25_am5,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as valeurfonc_median_am5,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q75_am5,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as pxm2_q25_am5,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as pxm2_median_am5,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as pxm2_q75_am5,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as sbati_sum_am5,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') AND periode_cst IN (''>= 2013'')) as sbati_median_am5
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') ) as nbtrans_amx,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') ) as valeurfonc_sum_amx,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') ) as valeurfonc_q25_amx,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') ) as valeurfonc_median_amx,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') ) as valeurfonc_q75_amx,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') ) as pxm2_q25_amx,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') ) as pxm2_median_amx,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') ) as pxm2_q75_amx,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') ) as sbati_sum_amx,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Moyen'') ) as sbati_median_amx
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as nbtrans_ag1,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as valeurfonc_sum_ag1,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as valeurfonc_q25_ag1,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as valeurfonc_median_ag1,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as valeurfonc_q75_ag1,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as pxm2_q25_ag1,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as pxm2_median_ag1,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as pxm2_q75_ag1,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as sbati_sum_ag1,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''< 1945'')) as sbati_median_ag1
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as nbtrans_ag2,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as valeurfonc_sum_ag2,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q25_ag2,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as valeurfonc_median_ag2,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as valeurfonc_q75_ag2,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as pxm2_q25_ag2,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as pxm2_median_ag2,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as pxm2_q75_ag2,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as sbati_sum_ag2,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1945-1960'')) as sbati_median_ag2
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as nbtrans_ag3,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as valeurfonc_sum_ag3,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q25_ag3,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as valeurfonc_median_ag3,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as valeurfonc_q75_ag3,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as pxm2_q25_ag3,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as pxm2_median_ag3,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as pxm2_q75_ag3,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as sbati_sum_ag3,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1961-1974'')) as sbati_median_ag3
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as nbtrans_ag4,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as valeurfonc_sum_ag4,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q25_ag4,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as valeurfonc_median_ag4,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as valeurfonc_q75_ag4,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as pxm2_q25_ag4,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as pxm2_median_ag4,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as pxm2_q75_ag4,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as sbati_sum_ag4,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''1975-2012'')) as sbati_median_ag4
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as nbtrans_ag5,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as valeurfonc_sum_ag5,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q25_ag5,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as valeurfonc_median_ag5,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as valeurfonc_q75_ag5,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as pxm2_q25_ag5,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as pxm2_median_ag5,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as pxm2_q75_ag5,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as sbati_sum_ag5,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') AND periode_cst IN (''>= 2013'')) as sbati_median_ag5
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') ) as nbtrans_agx,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') ) as valeurfonc_sum_agx,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') ) as valeurfonc_q25_agx,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') ) as valeurfonc_median_agx,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') ) as valeurfonc_q75_agx,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') ) as pxm2_q25_agx,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') ) as pxm2_median_agx,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') ) as pxm2_q75_agx,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') ) as sbati_sum_agx,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'') AND taille IN (''Grand'') ) as sbati_median_agx
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''< 1945'')) as nbtrans_ax1,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''< 1945'')) as valeurfonc_sum_ax1,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''< 1945'')) as valeurfonc_q25_ax1,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''< 1945'')) as valeurfonc_median_ax1,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''< 1945'')) as valeurfonc_q75_ax1,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''< 1945'')) as pxm2_q25_ax1,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''< 1945'')) as pxm2_median_ax1,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''< 1945'')) as pxm2_q75_ax1,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''< 1945'')) as sbati_sum_ax1,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''< 1945'')) as sbati_median_ax1
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1945-1960'')) as nbtrans_ax2,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1945-1960'')) as valeurfonc_sum_ax2,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1945-1960'')) as valeurfonc_q25_ax2,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1945-1960'')) as valeurfonc_median_ax2,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1945-1960'')) as valeurfonc_q75_ax2,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1945-1960'')) as pxm2_q25_ax2,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1945-1960'')) as pxm2_median_ax2,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1945-1960'')) as pxm2_q75_ax2,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1945-1960'')) as sbati_sum_ax2,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1945-1960'')) as sbati_median_ax2
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1961-1974'')) as nbtrans_ax3,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1961-1974'')) as valeurfonc_sum_ax3,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1961-1974'')) as valeurfonc_q25_ax3,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1961-1974'')) as valeurfonc_median_ax3,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1961-1974'')) as valeurfonc_q75_ax3,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1961-1974'')) as pxm2_q25_ax3,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1961-1974'')) as pxm2_median_ax3,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1961-1974'')) as pxm2_q75_ax3,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1961-1974'')) as sbati_sum_ax3,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1961-1974'')) as sbati_median_ax3
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1975-2012'')) as nbtrans_ax4,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1975-2012'')) as valeurfonc_sum_ax4,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1975-2012'')) as valeurfonc_q25_ax4,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1975-2012'')) as valeurfonc_median_ax4,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1975-2012'')) as valeurfonc_q75_ax4,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1975-2012'')) as pxm2_q25_ax4,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1975-2012'')) as pxm2_median_ax4,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1975-2012'')) as pxm2_q75_ax4,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1975-2012'')) as sbati_sum_ax4,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''1975-2012'')) as sbati_median_ax4
	,
	---- Par taille et période de construction

	count(idmutinvar) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''>= 2013'')) as nbtrans_ax5,   
	sum(valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''>= 2013'')) as valeurfonc_sum_ax5,
	percentile_cont(0.25) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''>= 2013'')) as valeurfonc_q25_ax5,
	percentile_cont(0.5) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''>= 2013'')) as valeurfonc_median_ax5,
	percentile_cont(0.75) within group (order by valeurfonc) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''>= 2013'')) as valeurfonc_q75_ax5,
	percentile_cont(0.25) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''>= 2013'')) as pxm2_q25_ax5,
	percentile_cont(0.5) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''>= 2013'')) as pxm2_median_ax5,
	percentile_cont(0.75) within group (order by pxm2) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''>= 2013'')) as pxm2_q75_ax5,
	sum(sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''>= 2013'')) as sbati_sum_ax5,
	percentile_cont(0.5) within group (order by sbati) FILTER (WHERE codtypbien IN (''1210'',''12110'',''12120'',''12130'',''12111'',''12112'',''12113'',''12114'',''12115'',''12121'',''12122'',''12123'',''12124'',''12125'',''12131'',''12132'',''12133'',''12134'',''12135'')  AND periode_cst IN (''>= 2013'')) as sbati_median_ax5
	,
	-- Appartements à usage professionel (code 1214)

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''1214'') as nbtrans_cod1214,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''1214'') as valeurfonc_sum_cod1214,

	-- Dépendances

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''13%'') as nbtrans_cod13,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''13%'') as valeurfonc_sum_cod13,

	---- Garages

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''1311'') as nbtrans_cod1311,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''1311'') as valeurfonc_sum_cod1311,

	-- Activités

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''14%'') as nbtrans_cod14,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''14%'') as valeurfonc_sum_cod14,

	---- Secondaire

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''142'') as nbtrans_cod142,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''142'') as valeurfonc_sum_cod142,

	---- Tertiaire

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''143'') as nbtrans_cod143,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''143'') as valeurfonc_sum_cod143,

	---- Mixte

	count(idmutinvar) FILTER (WHERE codtypbien LIKE ''149'') as nbtrans_cod149,
	sum(valeurfonc) FILTER (WHERE codtypbien LIKE ''149'') as valeurfonc_sum_cod149,
	
	b.geom

FROM temp a,zonage b
WHERE (St_Intersects(a.geomlocmut,b.geom) OR St_Intersects(ST_Centroid(a.geomparmut),b.geom)) AND anneemut between '||an1||' AND '||an2||'
AND filtre = ''0'' AND devenir = ''S'' AND nbcomm=''1''
GROUP BY id,nom,geom';


END;
$BODY$;

--ALTER FUNCTION public.__ind_niv1(text, text, text) OWNER TO postgres;

