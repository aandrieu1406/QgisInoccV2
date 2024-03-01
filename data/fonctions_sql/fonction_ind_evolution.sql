-- FUNCTION: public.__ind_evolution(text, text, text)

-- DROP FUNCTION IF EXISTS public.__ind_evolution(text, text, text);

CREATE OR REPLACE FUNCTION public.__ind_evolution(
	an1 text,
	an2 text,
	dat text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    --VOLATILE PARALLEL UNSAFE
AS $BODY$

begin

EXECUTE 'DROP TABLE IF EXISTS public.ind_evolution_'||dat||' CASCADE';
EXECUTE 'CREATE TABLE public.ind_evolution_'||dat||' AS
SELECT 
    row_number() over() as id,
	a.anneemut,
	nb_appt,
	prix_median_appt,
	prix_median_m2_appt,
	nb_maison,
	prix_median_maison,
	prix_median_m2_maison,
	nb_tab,
    surf_median_tab,
	prix_median_tab,
	prix_median_m2_tab
FROM
(SELECT
    anneemut
FROM
    dvf.mutation
GROUP BY anneemut
ORDER BY anneemut) a
LEFT JOIN
(SELECT anneemut,
    case when codtypbien like ''121%'' then ''appartement seul'' end as codtypbien2,
    sum(nblocapt) AS nb_appt,
    round(percentile_disc(0.5) within group (order by valeurfonc)) AS prix_median_appt,
    round(percentile_disc(0.5) within group (order by valeurfonc/sbati)) AS prix_median_m2_appt
   FROM dvf.mutation m, public.zonage p
  WHERE (idnatmut = ANY (ARRAY[1, 2, 4])) AND codtypbien like ''121%''::text AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND sbati > 9::numeric
		and st_intersects(m.geomlocmut,p.geom)
		and filtre = ''0''
  GROUP BY anneemut, codtypbien2
  ORDER BY anneemut) b
ON a.anneemut=b.anneemut
LEFT JOIN
(SELECT anneemut,
    case when codtypbien like ''111%'' then ''maison seule'' end as codtypbien2,
    sum(nblocmai) AS nb_maison,
    round(percentile_disc(0.5) within group (order by valeurfonc)) AS prix_median_maison,
    round(percentile_disc(0.5) within group (order by valeurfonc/sbati)) AS prix_median_m2_maison
   FROM dvf.mutation m, public.zonage p
  WHERE (idnatmut = ANY (ARRAY[1, 2, 4])) AND codtypbien like ''111%''::text AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND sbati > 9::numeric
		and st_intersects(m.geomlocmut,p.geom)
		and filtre = ''0''
  GROUP BY anneemut, codtypbien2
  ORDER BY anneemut) c
  ON a.anneemut=c.anneemut
LEFT JOIN
(SELECT anneemut,
    case when codtypbien like ''2%'' then ''TAB'' end as codtypbien2,
    count(*) AS nb_tab,
    round(percentile_disc(0.5) within group (order by sterr)) as surf_median_tab,
    round(percentile_disc(0.5) within group (order by valeurfonc)) AS prix_median_tab,
    round(percentile_disc(0.5) within group (order by valeurfonc/sterr)) AS prix_median_m2_tab
   FROM dvf.mutation m, public.zonage p
  WHERE (idnatmut = ANY (ARRAY[1, 2, 4])) AND codtypbien like ''2%'' and segmtab in (''3'',''4'')
		AND codtypproa = ''X1'' and filtre = ''0''
		and st_intersects(ST_Centroid(m.geomparmut),p.geom)		
  GROUP BY anneemut, codtypbien2
  ORDER BY anneemut) d
  ON a.anneemut=d.anneemut';
  
EXECUTE 'ALTER TABLE public.ind_evolution_'||dat||' ADD CONSTRAINT ind_evolution_'||dat||'_pkey PRIMARY KEY (id)';

EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.id IS ''identifiant''';
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.anneemut IS ''Année de la mutation''';
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.nb_appt IS ''Nombre d''''appartements vendus (seuls) par année''';
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.prix_median_appt IS ''Prix médian des appartements vendus seuls par année''';
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.prix_median_m2_appt IS ''Prix médian au m² des appartements vendus seuls par année''';
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.nb_maison IS ''Nombre de maisons vendues (seules) par année'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.prix_median_maison IS ''Prix médian des maisons vendues seules par année'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.prix_median_m2_maison IS ''Prix médian au m² des maisons vendues seules par année''';
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.nb_tab IS ''Nombre de terrains à bâtir vendus par année''';
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.surf_median_tab IS ''Surface médiane des terrains à bâtir vendus seuls par année''';
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.prix_median_tab IS ''Prix médian des terrains à bâtir vendus seuls par année''';
EXECUTE 'COMMENT ON COLUMN public.ind_evolution_'||dat||'.prix_median_m2_tab IS ''Prix médian au m² des terrains à bâtir vendus seuls par année'''; 
 

END;
$BODY$;

--ALTER FUNCTION public.__ind_evolution(text, text, text) OWNER TO postgres;
