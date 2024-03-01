-- FUNCTION: public.__ind_maison(text, text, text)

-- DROP FUNCTION IF EXISTS public.__ind_maison(text, text, text);

CREATE OR REPLACE FUNCTION public.__ind_maison(
	an1 text,
	an2 text,
	dat text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    --VOLATILE PARALLEL UNSAFE
AS $BODY$

begin

EXECUTE 'DROP TABLE IF EXISTS public.ind_maison_'||dat||' CASCADE';
EXECUTE 'CREATE TABLE public.ind_maison_'||dat||' AS
SELECT 
    p.periode,
    p.id,
    p.nom,
	nb_maison,
    nb_maison_seule,
    surf_maison_seule_median,
    maison_seule_prix_median,
    maison_seule_euro_m2_median,
    surf_terrain_maison_seule_median,
    nb_maison_vefa,
    surf_maison_vefa_median,
    maison_vefa_prix_median,
    maison_vefa_euro_m2_median,
    surf_terrain_maison_vefa_median,
    nb_maison_neuve,
    surf_maison_neuve_median,
    maison_neuve_prix_median,
    maison_neuve_euro_m2_median,
    surf_terrain_maison_neuve_median, 
    nb_maison_recente,
    surf_maison_recente_median,
    maison_recente_prix_median,
    maison_recente_euro_m2_median,
    surf_terrain_maison_recente_median,
    nb_maison_ancienne,
    surf_maison_ancienne_median,
    maison_ancienne_prix_median,
    maison_ancienne_euro_m2_median,
    surf_terrain_maison_ancienne_median,
    nb_maison_t1_vefa,
    surf_maison_t1_vefa_median,
    maison_t1_vefa_prix_median,
    maison_t1_vefa_euro_m2_median,
    surf_terrain_maison_t1_vefa_median,
    nb_maison_t2_vefa,
    surf_maison_t2_vefa_median,
    maison_t2_vefa_prix_median,
    maison_t2_vefa_euro_m2_median,
    surf_terrain_maison_t2_vefa_median,
    nb_maison_t3_vefa,
    surf_maison_t3_vefa_median,
    maison_t3_vefa_prix_median,
    maison_t3_vefa_euro_m2_median,
    surf_terrain_maison_t3_vefa_median,
    nb_maison_t4_vefa,
    surf_maison_t4_vefa_median,
    maison_t4_vefa_prix_median,
    maison_t4_vefa_euro_m2_median,
    surf_terrain_maison_t4_vefa_median,
    nb_maison_t1,
    surf_maison_t1_median,
    maison_t1_prix_median,
    maison_t1_euro_m2_median,
    surf_terrain_maison_t1_median,
    nb_maison_t2,
    surf_maison_t2_median,
    maison_t2_prix_median,
    maison_t2_euro_m2_median,
    surf_terrain_maison_t2_median,
    nb_maison_t3,
    surf_maison_t3_median,
    maison_t3_prix_median,
    maison_t3_euro_m2_median,
    surf_terrain_maison_t3_median,
    nb_maison_t4,
    surf_maison_t4_median,
    maison_t4_prix_median,
    maison_t4_euro_m2_median,
    surf_terrain_maison_t4_median,	
	p.geom
FROM
    (SELECT 
        id, nom, periode, geom
    FROM public.zonage) p
    LEFT JOIN
        (SELECT 
            b.id,
            sum(nblocmai) as nb_maison
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''11%''
        AND idnatmut IN (1,2,4)
        AND st_intersects(a.geomlocmut,b.geom) 
        group by b.id, b.nom) rr
    ON p.id=rr.id	
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_maison_seule,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_seule_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_seule_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_seule_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_seule_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''111%'' AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) r
    ON p.id=r.id
    LEFT JOIN
        (SELECT 
			-- Maison vendue en VEFA
            b.id,
            count(*) AS nb_maison_vefa,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_vefa_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_vefa_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_vefa_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_vefa_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien = ''1111'' AND vefa IS TRUE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) s
    ON p.id=s.id
    LEFT JOIN
        (SELECT 
			-- Maison neuve (moins de 1 an)
            b.id,
            count(*) AS nb_maison_neuve,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_neuve_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_neuve_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_neuve_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_neuve_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien = ''1111'' AND vefa IS FALSE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) t
    ON p.id=t.id
    LEFT JOIN
        (SELECT 
			-- Maison récente (entre 2 et 4 ans)
            b.id,
            count(*) AS nb_maison_recente,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_recente_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_recente_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_recente_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_recente_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien = ''1112'' AND sbati > 9 
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) u
    ON p.id=u.id
    LEFT JOIN
        (SELECT 
			-- maison ancienne (plus de 5 ans) + maison d_age indéterminé (voir si on garde)
            b.id,
            count(*) AS nb_maison_ancienne,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_ancienne_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_ancienne_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_ancienne_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_ancienne_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien IN (''1110'',''1113'') AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) v
    ON p.id=v.id
    LEFT JOIN
        (SELECT 
			-- maison vendue en VEFA
            b.id,
            count(*) AS nb_maison_t1_vefa,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_t1_vefa_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_t1_vefa_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_t1_vefa_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_t1_vefa_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''111%'' AND vefa IS TRUE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND nbmai1pp>0 AND nbmai2pp=0 AND nbmai3pp=0 AND nbmai4pp=0 AND nbmai5pp=0 
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) w
    ON p.id=w.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_maison_t2_vefa,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_t2_vefa_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_t2_vefa_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_t2_vefa_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_t2_vefa_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''111%'' AND vefa IS TRUE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND nbmai1pp=0 AND nbmai2pp>0 AND nbmai3pp=0 AND nbmai4pp=0 AND nbmai5pp=0 
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) x
    ON p.id=x.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_maison_t3_vefa,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_t3_vefa_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_t3_vefa_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_t3_vefa_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_t3_vefa_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''111%'' AND vefa IS TRUE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND nbmai1pp=0 AND nbmai2pp=0 AND nbmai3pp>0 AND nbmai4pp=0 AND nbmai5pp=0 
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) y
    ON p.id=y.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_maison_t4_vefa,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_t4_vefa_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_t4_vefa_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_t4_vefa_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_t4_vefa_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''111%'' AND vefa IS TRUE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND nbmai1pp=0 AND nbmai2pp=0 AND nbmai3pp=0 AND (nbmai4pp>0 OR nbmai5pp>0) 
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) z
    ON p.id=z.id
    LEFT JOIN
        (SELECT 
			-- maison vendue hors VEFA
            b.id,
            count(*) AS nb_maison_t1,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_t1_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_t1_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_t1_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_t1_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''111%'' AND vefa IS FALSE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND nbmai1pp>0 AND nbmai2pp=0 AND nbmai3pp=0 AND nbmai4pp=0 AND nbmai5pp=0 
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) aa
    ON p.id=aa.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_maison_t2,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_t2_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_t2_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_t2_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_t2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''111%'' AND vefa IS FALSE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND nbmai1pp=0 AND nbmai2pp>0 AND nbmai3pp=0 AND nbmai4pp=0 AND nbmai5pp=0 
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) bb
    ON p.id=bb.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_maison_t3,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_t3_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_t3_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_t3_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_t3_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''111%'' AND vefa IS FALSE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND nbmai1pp=0 AND nbmai2pp=0 AND nbmai3pp>0 AND nbmai4pp=0 AND nbmai5pp=0 
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) cc
    ON p.id=cc.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nb_maison_t4,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS surf_maison_t4_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS maison_t4_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS maison_t4_euro_m2_median,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS surf_terrain_maison_t4_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''111%'' AND vefa IS FALSE AND sbati > 9
        AND (ffnbloch <> 0 OR ffnbloch IS NULL) AND devenir=''S'' AND filtre=''0'' AND idnatmut IN (1,2,4)
        AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
        AND nbmai1pp=0 AND nbmai2pp=0 AND nbmai3pp=0 AND (nbmai4pp>0 OR nbmai5pp>0) 
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) dd
    ON p.id=dd.id	
    ORDER BY p.nom';

EXECUTE 'COMMENT ON TABLE public.ind_maison_'||dat||' IS ''Indicateurs sur les maisons''';
 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison IS ''Nombre total de vente de maisons (avec ou sans dépendance)'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_seule IS ''Nombre de vente de maisons (avec ou sans dépendance)'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_seule_median IS ''Superficie médiane des maisons vendues (avec ou sans dépendance)'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_seule_prix_median IS ''Prix médian des maisons vendues (avec ou sans dépendance)'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_seule_euro_m2_median IS ''Prix médian au m² des maisons vendues (avec ou sans dépendance)'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_seule_median IS ''Superficie médiane de terrain des maisons vendues (avec ou sans dépendance)'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_vefa IS ''Nombre de vente de maisons VEFA''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_vefa_median IS ''Superficie médiane bâtie de maisons VEFA vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_vefa_prix_median IS ''Prix médian de maisons VEFA vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_vefa_euro_m2_median IS ''Prix médian au m² bâti de maisons VEFA vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_vefa_median IS ''Superficie médiane de terrain de maisons VEFA vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_neuve IS ''Nombre de vente de maisons neuves''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_neuve_median IS ''Superficie médiane bâtie de maisons neuves vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_neuve_prix_median IS ''Prix médian de maisons neuves vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_neuve_euro_m2_median IS ''Prix médian au m² bâti de maisons neuves vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_neuve_median IS ''Superficie médiane de terrain de maisons neuves vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_recente IS ''Nombre de vente de maisons récentes''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_recente_median IS ''Superficie médiane bâtie de maisons récentes vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_recente_prix_median IS ''Prix médian de maisons récentes vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_recente_euro_m2_median IS ''Prix médian au m² bâti de maisons récentes vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_recente_median IS ''Superficie médiane de terrain de maisons récentes vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_ancienne IS ''Nombre de vente de maisons anciennes''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_ancienne_median IS ''Superficie médiane bâtie de maisons anciennes vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_ancienne_prix_median IS ''Prix médian de maisons anciennes vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_ancienne_euro_m2_median IS ''Prix médian au m² bâti de maisons anciennes vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_ancienne_median IS ''Superficie médiane de terrain de maisons anciennes vendues''';  
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_t1_vefa IS ''Nombre de vente de maisons VEFA de type T1'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_t1_vefa_median IS ''Superficie médiane bâtie de maisons VEFA vendues de type T1'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t1_vefa_prix_median IS ''Prix médian de maisons VEFA vendues de type T1'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t1_vefa_euro_m2_median IS ''Prix médian au m² bâti de maisons VEFA vendues de type T1'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_t1_vefa_median IS ''Superficie médiane de terrain de maisons VEFA vendues de type T1'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_t2_vefa IS ''Nombre de vente de maisons VEFA de type T2'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_t2_vefa_median IS ''Superficie médiane bâtie de maisons VEFA vendues de type T2'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t2_vefa_prix_median IS ''Prix médian de maisons VEFA vendues de type T2'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t2_vefa_euro_m2_median IS ''Prix médian au m² bâti de maisons VEFA vendues de type T2'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_t2_vefa_median IS ''Superficie médiane de terrain de maisons VEFA vendues de type T2'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_t3_vefa IS ''Nombre de vente de maisons VEFA de type T3'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_t3_vefa_median IS ''Superficie médiane bâtie de maisons VEFA vendues de type T3'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t3_vefa_prix_median IS ''Prix médian de maisons VEFA vendues de type T3'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t3_vefa_euro_m2_median IS ''Prix médian au m² bâti de maisons VEFA vendues de type T3'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_t3_vefa_median IS ''Superficie médiane de terrain de maisons VEFA vendues de type T3'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_t4_vefa IS ''Nombre de vente de maisons VEFA de type T4'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_t4_vefa_median IS ''Superficie médiane bâtie de maisons VEFA vendues de type T4'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t4_vefa_prix_median IS ''Prix médian de maisons VEFA vendues de type T4'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t4_vefa_euro_m2_median IS ''Prix médian au m² bâti de maisons VEFA vendues de type T4'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_t4_vefa_median IS ''Superficie médiane de terrain de maisons VEFA vendues de type T4'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_t1 IS ''Nombre de vente de maisons HORS VEFA de type T1'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_t1_median IS ''Superficie médiane bâtie de maisons HORS VEFA vendues de type T1'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t1_prix_median IS ''Prix médian de maisons HORS VEFA vendues de type T1'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t1_euro_m2_median IS ''Prix médian au m² bâti de maisons HORS VEFA vendues de type T1'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_t1_median IS ''Superficie médiane de terrain de maisons HORS VEFA vendues de type T1'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_t2 IS ''Nombre de vente de maisons HORS VEFA de type T2'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_t2_median IS ''Superficie médiane bâtie de maisons HORS VEFA vendues de type T2'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t2_prix_median IS ''Prix médian de maisons HORS VEFA vendues de type T2'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t2_euro_m2_median IS ''Prix médian au m² bâti de maisons HORS VEFA vendues de type T2'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_t2_median IS ''Superficie médiane de terrain de maisons HORS VEFA vendues de type T2'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_t3 IS ''Nombre de vente de maisons HORS VEFA de type T3'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_t3_median IS ''Superficie médiane bâtie de maisons HORS VEFA vendues de type T3'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t3_prix_median IS ''Prix médian de maisons HORS VEFA vendues de type T3'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t3_euro_m2_median IS ''Prix médian au m² bâti de maisons HORS VEFA vendues de type T3'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_t3_median IS ''Superficie médiane de terrain de maisons HORS VEFA vendues de type T3'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.nb_maison_t4 IS ''Nombre de vente de maisons HORS VEFA de type T4'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_maison_t4_median IS ''Superficie médiane bâtie de maisons HORS VEFA vendues de type T4'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t4_prix_median IS ''Prix médian de maisons HORS VEFA vendues de type T4'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.maison_t4_euro_m2_median IS ''Prix médian au m² bâti de maisons HORS VEFA vendues de type T4'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_maison_'||dat||'.surf_terrain_maison_t4_median IS ''Superficie médiane de terrain de maisons HORS VEFA vendues de type T4''';   
END;
$BODY$;

--ALTER FUNCTION public.__ind_maison(text, text, text) OWNER TO postgres;
