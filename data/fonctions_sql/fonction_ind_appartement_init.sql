-- FUNCTION: public.__ind_appartement(text, text, text)

-- DROP FUNCTION IF EXISTS public.__ind_appartement(text, text, text);

CREATE OR REPLACE FUNCTION public.__ind_appartement(
	an1 text,
	an2 text,
	dat text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    --VOLATILE PARALLEL UNSAFE
AS $BODY$

begin

EXECUTE 'DROP TABLE IF EXISTS public.ind_appartement_'||dat||' CASCADE';
EXECUTE 'CREATE TABLE public.ind_appartement_'||dat||' AS
SELECT 
    p.periode,
    p.id,
    p.nom,
	nb_appart,
    nb_appart_seul,
    surf_appart_median,
    prix_median,
    prix_euro_m2_median,
    nb_appart_seul_sans_garage,
    surf_appart_sans_garage_median,
    appart_sans_garage_prix_median,
    appart_sans_garage_prix_euro_m2_median,
    nb_appart_seul_1_garage,
    surf_appart_1_garage_median,
    appart_1_garage_prix_median,
    appart_1_garage_prix_euro_m2_median,
    nb_appart_seul_2_garage,
    surf_appart_2_garage_median,
    appart_2_garage_prix_median,
    appart_2_garage_prix_euro_m2_median,
    nb_appart_vefa,
    surf_appart_vefa_median,
    appart_vefa_prix_median,
    appart_vefa_euro_m2_median,
    nb_appart_neuf,
    surf_appart_neuf_median,
    appart_neuf_prix_median,
    appart_neuf_euro_m2_median,   
    nb_appart_recent,
    surf_appart_recent_median,
    appart_recent_prix_median,
    appart_recent_euro_m2_median,   
    nb_appart_ancien,
    surf_appart_ancien_median,
    appart_ancien_prix_median,
    appart_ancien_euro_m2_median,
    nb_appart_vefa_t1,
    surf_appart_vefa_t1_median,
    appart_vefa_t1_prix_median,
    appart_vefa_t1_euro_m2_median,
    nb_appart_vefa_t2,
    surf_appart_vefa_t2_median,
    appart_vefa_t2_prix_median,
    appart_vefa_t2_euro_m2_median,
    nb_appart_vefa_t3,
    surf_appart_vefa_t3_median,
    appart_vefa_t3_prix_median,
    appart_vefa_t3_euro_m2_median,
    nb_appart_vefa_t4,
    surf_appart_vefa_t4_median,
    appart_vefa_t4_prix_median,
    appart_vefa_t4_euro_m2_median,
    nb_appart_hors_vefa_t1,
    surf_appart_hors_vefa_t1_median,
    appart_hors_vefa_t1_prix_median,
    appart_hors_vefa_t1_euro_m2_median,
    nb_appart_hors_vefa_t2,
    surf_appart_hors_vefa_t2_median,
    appart_hors_vefa_t2_prix_median,
    appart_hors_vefa_t2_euro_m2_median,
    nb_appart_hors_vefa_t3,
    surf_appart_hors_vefa_t3_median,
    appart_hors_vefa_t3_prix_median,
    appart_hors_vefa_t3_euro_m2_median,
    nb_appart_hors_vefa_t4,
    surf_appart_hors_vefa_t4_median,
    appart_hors_vefa_t4_prix_median,
    appart_hors_vefa_t4_euro_m2_median,
	p.geom


		
		
FROM
    (SELECT 
        id, nom, periode, geom
    FROM public.zonage) p
    LEFT JOIN
        (SELECT 
            b.id,
            sum(nblocapt) as nb_appart
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''12%''
        AND idnatmut IN (1,2,4)
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) aa
    ON p.id=aa.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_seul,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''121%'' AND sbati > 9
        AND filtre=''0'' AND (ffnbloch != 0 or ffnbloch is null)
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) a
    ON p.id=a.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_seul_sans_garage,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_sans_garage_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_sans_garage_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_sans_garage_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''121%'' AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND ffnbpgarag = 0 AND nblocapt = 1
        AND filtre=''0''
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) b
    ON p.id=b.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_seul_1_garage,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_1_garage_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_1_garage_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_1_garage_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''121%'' AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND ffnbpgarag = 1 AND nblocapt = 1
        AND filtre=''0''
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) c
    ON p.id=c.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_seul_2_garage,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_2_garage_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_2_garage_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_2_garage_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''121%'' AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND ffnbpgarag = 2 AND nblocapt = 1
        AND filtre=''0''
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) d
    ON p.id=d.id    
    LEFT JOIN
        (SELECT 
			-- appartement vendu en VEFA
            b.id,
            count(*) AS nb_appart_vefa,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_vefa_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_vefa_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_vefa_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''1211%'' AND vefa IS TRUE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) e
    ON p.id=e.id
    LEFT JOIN
        (SELECT 
			-- appartement neuf (moins de 1 an)
            b.id,
            count(*) AS nb_appart_neuf,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_neuf_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_neuf_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_neuf_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''1211%'' AND vefa IS FALSE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) f
    ON p.id=f.id
    LEFT JOIN
        (SELECT 
			-- appartement récent (entre 2 et 4 ans)
            b.id,
            count(*) AS nb_appart_recent,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_recent_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_recent_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_recent_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''1212%'' AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) g
    ON p.id=g.id    
    LEFT JOIN
        (SELECT 
			-- appartement ancien (plus de 5 ans) + appartement d_age indéterminé (voir si on garde)
            b.id,
            count(*) AS nb_appart_ancien,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_ancien_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_ancien_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_ancien_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND (codtypbien LIKE ''1210%'' OR codtypbien LIKE ''1213%'') AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) h
    ON p.id=h.id  
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_vefa_t1,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_vefa_t1_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_vefa_t1_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_vefa_t1_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien = ''12111'' AND sbati > 9 AND vefa IS TRUE
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) i
    ON p.id=i.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_vefa_t2,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_vefa_t2_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_vefa_t2_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_vefa_t2_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien = ''12112'' AND sbati > 9 AND vefa IS TRUE
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) j
    ON p.id=j.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_vefa_t3,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_vefa_t3_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_vefa_t3_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_vefa_t3_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien = ''12113'' AND sbati > 9 AND vefa IS TRUE
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) k
    ON p.id=k.id    
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_vefa_t4,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_vefa_t4_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_vefa_t4_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_vefa_t4_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND (codtypbien = ''12114'' OR codtypbien = ''12115'') AND sbati > 9 AND vefa IS TRUE
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) l
    ON p.id=l.id  
    LEFT JOIN
        (SELECT 
		-- appartement vendu hors VEFA
            b.id,
            count(*) AS nb_appart_hors_vefa_t1,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_hors_vefa_t1_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_hors_vefa_t1_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_hors_vefa_t1_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien IN (''12111'',''12121'',''12131'') AND sbati > 9 AND vefa IS FALSE
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) m
    ON p.id=m.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_hors_vefa_t2,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_hors_vefa_t2_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_hors_vefa_t2_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_hors_vefa_t2_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien IN (''12112'',''12122'',''12132'') AND sbati > 9 AND vefa IS FALSE
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) n
    ON p.id=n.id    
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_hors_vefa_t3,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_hors_vefa_t3_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_hors_vefa_t3_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_hors_vefa_t3_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien IN (''12113'',''12123'',''12133'') AND sbati > 9 AND vefa IS FALSE
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) o
    ON p.id=o.id  
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_appart_hors_vefa_t4,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_appart_hors_vefa_t4_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS appart_hors_vefa_t4_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS appart_hors_vefa_t4_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien IN (''12114'',''12124'',''12134'',''12115'',''12125'',''12135'') AND sbati > 9 AND vefa IS FALSE
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) q
    ON p.id=q.id
    ORDER BY p.nom';

EXECUTE 'COMMENT ON TABLE public.ind_appartement_'||dat||' IS ''Nombre d''''appartements vendus seuls (avec ou sans dépendance), superficie médiane, prix médian, prix médian €/m²''';
 
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.id IS ''identifiant du zonage (code INSEE pour les communes ou code IRIS)''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nom IS ''nom du zonage''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.periode IS ''Période de traitement (bornes incluses)''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart IS ''Nombre total d''''appartements (avec ou sans dépendance)''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_seul IS ''Nombre d''''appartements vendus seuls (avec ou sans dépendance)''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_median IS ''superficie médiane des appartements vendus seuls (avec ou sans dépendance)''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.prix_median IS ''prix médian des appartements vendus seuls (avec ou sans dépendance)''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.prix_euro_m2_median IS ''prix médian €/m² des appartements vendus seuls (avec ou sans dépendance)''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_seul_sans_garage IS ''Nombre d''''appartements vendus seuls sans garage''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_sans_garage_median IS ''Superficie médiane des appartements vendus seuls sans garage''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_sans_garage_prix_median IS ''Prix médian des appartements vendus seuls sans garage''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_sans_garage_prix_euro_m2_median IS ''Prix médian au m² des appartements vendus seuls sans garage''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_seul_1_garage IS ''Nombre d''''appartements vendus seuls avec 1 garage''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_1_garage_median IS ''Superficie médiane des appartements vendus seuls avec 1 garage''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_1_garage_prix_median IS ''Prix médian des appartements vendus seuls avec 1 garage''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_1_garage_prix_euro_m2_median IS ''Prix médian au m² des appartements vendus seuls avec 1 garage''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_seul_2_garage IS ''Nombre d''''appartements vendus seuls avec 2 garages''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_2_garage_median IS ''Superficie médiane des appartements vendus seuls avec 2 garages''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_2_garage_prix_median IS ''Prix médian des appartements vendus seuls avec 2 garages''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_2_garage_prix_euro_m2_median IS ''Prix médian au m² des appartements vendus seuls avec 2 garages''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_vefa IS ''Nombre de vente d''''appartements VEFA''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_vefa_median IS ''Superficie médiane des appartements VEFA''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_vefa_prix_median IS ''Prix médian des appartements VEFA''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_vefa_euro_m2_median IS ''Prix médian au m² des appartements VEFA''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_neuf IS ''Nombre de vente d''''appartements neufs''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_neuf_median IS ''Superficie médiane des appartements neufs''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_neuf_prix_median IS ''Prix médian des appartements neufs''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_neuf_euro_m2_median IS ''Prix médian au m² des appartements neufs''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_recent IS ''Nombre de vente d''''appartements récents''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_recent_median IS ''Superficie médiane des appartements récents''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_recent_prix_median IS ''Prix médian des appartements récents''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_recent_euro_m2_median IS ''Prix médian au m² des appartements récents''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_ancien IS ''Nombre de vente d''''appartements anciens''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_ancien_median IS ''Superficie médiane des appartements anciens''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_ancien_prix_median IS ''Prix médian des appartements anciens''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_ancien_euro_m2_median IS ''Prix médian au m² des appartements anciens''';    
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_vefa_t1 IS ''Nombre de vente d''''appartements VEFA de type T1''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_vefa_t1_median IS ''Superficie médiane des appartements VEFA vendus de type T1''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_vefa_t1_prix_median IS ''Prix médian des appartements VEFA vendus de type T1''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_vefa_t1_euro_m2_median IS ''Prix médian au m² des appartements VEFA vendus de type T1''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_vefa_t2 IS ''Nombre de vente d''''appartements VEFA de type T2''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_vefa_t2_median IS ''Superficie médiane des appartements VEFA vendus de type T2''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_vefa_t2_prix_median IS ''Prix médian des appartements VEFA vendus de type T2''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_vefa_t2_euro_m2_median IS ''Prix médian au m² des appartements VEFA vendus de type T2''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_vefa_t3 IS ''Nombre de vente d''''appartements VEFA de type T3''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_vefa_t3_median IS ''Superficie médiane des appartements VEFA vendus de type T3''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_vefa_t3_prix_median IS ''Prix médian des appartements VEFA vendus de type T3''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_vefa_t3_euro_m2_median IS ''Prix médian au m² des appartements VEFA vendus de type T3''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_vefa_t4 IS ''Nombre de vente d''''appartements VEFA de type T4 et plus''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_vefa_t4_median IS ''Superficie médiane des appartements VEFA vendus de type T4 et plus''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_vefa_t4_prix_median IS ''Prix médian des appartements VEFA vendus de type T4 et plus''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_vefa_t4_euro_m2_median IS ''Prix médian au m² des appartements VEFA vendus de type T4 et plus''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_hors_vefa_t1 IS ''Nombre de vente d''''appartements hors VEFA de type T1''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_hors_vefa_t1_median IS ''Superficie médiane des appartements hors VEFA vendus de type T1''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_hors_vefa_t1_prix_median IS ''Prix médian des appartements hors VEFA vendus de type T1''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_hors_vefa_t1_euro_m2_median IS ''Prix médian au m² des appartements hors VEFA vendus de type T1''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_hors_vefa_t2 IS ''Nombre de vente d''''appartements hors VEFA de type T2''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_hors_vefa_t2_median IS ''Superficie médiane des appartements hors VEFA vendus de type T2''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_hors_vefa_t2_prix_median IS ''Prix médian des appartements hors VEFA vendus de type T2''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_hors_vefa_t2_euro_m2_median IS ''Prix médian au m² des appartements hors VEFA vendus de type T2''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_hors_vefa_t3 IS ''Nombre de vente d''''appartements hors VEFA de type T3''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_hors_vefa_t3_median IS ''Superficie médiane des appartements hors VEFA vendus de type T3''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_hors_vefa_t3_prix_median IS ''Prix médian des appartements hors VEFA vendus de type T3''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_hors_vefa_t3_euro_m2_median IS ''Prix médian au m² des appartements hors VEFA vendus de type T3''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.nb_appart_hors_vefa_t4 IS ''Nombre de vente d''''appartements hors VEFA de type T4 et plus''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.surf_appart_hors_vefa_t4_median IS ''Superficie médiane des appartements hors VEFA vendus de type T4 et plus''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_hors_vefa_t4_prix_median IS ''Prix médian des appartements hors VEFA vendus de type T4 et plus''';
EXECUTE 'COMMENT ON COLUMN public.ind_appartement_'||dat||'.appart_hors_vefa_t4_euro_m2_median IS ''Prix médian au m² des appartements hors VEFA vendus de type T4 et plus'''; 

END;
$BODY$;

--ALTER FUNCTION public.__ind_appartement(text, text, text) OWNER TO postgres;
