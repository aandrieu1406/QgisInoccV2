-- FUNCTION: public.__ind_foncier(text, text, text)

-- DROP FUNCTION IF EXISTS public.__ind_foncier(text, text, text);

CREATE OR REPLACE FUNCTION public.__ind_foncier(
	an1 text,
	an2 text,
	dat text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    --VOLATILE PARALLEL UNSAFE
AS $BODY$

begin

EXECUTE 'DROP TABLE IF EXISTS public.ind_foncier_'||dat||' CASCADE';
EXECUTE 'CREATE TABLE public.ind_foncier_'||dat||' AS
SELECT 
    p.periode,
    p.id,
    p.nom,
	nb_terrain,
    tab_fp_nb,
    tab_fp_surf_terr_median,
    tab_fp_prix_median,
    tab_fp_prix_euro_m2_median,
    tab_pot_nb,
    tab_pot_surf_terr_median,
    tab_pot_prix_median,
    tab_pot_prix_euro_m2_median,
    terr_artif_nb,
    terr_artif_surf_terr_median,
    terr_artif_prix_median,
    terr_artif_prix_euro_m2_median,
    terr_agri_nb,
    terr_agri_surf_terr_median,
    terr_agri_prix_median,
    terr_agri_prix_euro_m2_median,
    terr_nat_nb,
    terr_nat_surf_terr_median,
    terr_nat_prix_median,
    terr_nat_prix_euro_m2_median,
    nonbati_indeterm_nb,
    nonbati_indeterm_surf_terr_median,
    nonbati_indeterm_prix_median,
    nonbati_indeterm_prix_euro_m2_median,
    tab_fp_X0_nb,
    tab_fp_X0_surf_terr_median,
    tab_fp_X0_prix_median,
    tab_fp_X0_prix_euro_m2_median,
	p.geom
FROM
    (SELECT 
        id, nom, periode, geom
    FROM public.zonage) p
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) as nb_terrain
        FROM dvf.mutation a, public.zonage b 
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''2%''
        AND idnatmut IN (1,2,4)
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom) 
        group by b.id, b.nom) aa
    ON p.id=aa.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS tab_fp_nb,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS tab_fp_surf_terr_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS tab_fp_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sterr))::INTEGER AS tab_fp_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''2%'' AND segmtab IN (''3'',''4'')
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom)
		AND idnatmut in (1,2,4) -- on ne prend en compte que les ventes
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
		AND sterr>0
        group by b.id, b.nom) a
    ON p.id=a.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS tab_pot_nb,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS tab_pot_surf_terr_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS tab_pot_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sterr))::INTEGER AS tab_pot_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''2%'' AND segmtab IN (''1'',''2'')
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom)
		AND idnatmut in (1,2,4) -- on ne prend en compte que les ventes
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null -- test CA
        group by b.id, b.nom) b
    ON p.id=b.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS terr_artif_nb,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS terr_artif_surf_terr_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS terr_artif_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sterr))::INTEGER AS terr_artif_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''22%''
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom)
		AND idnatmut in (1,2,4) -- on ne prend en compte que les ventes
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
		AND segmtab is null
		AND sterr>0		
        group by b.id, b.nom) c
    ON p.id=c.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS terr_agri_nb,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS terr_agri_surf_terr_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS terr_agri_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sterr))::INTEGER AS terr_agri_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''231%''
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom)
		AND idnatmut in (1,2,4) -- on ne prend en compte que les ventes
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
		AND segmtab is null
		AND sterr>0		
        group by b.id, b.nom) d
    ON p.id=d.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS terr_nat_nb,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS terr_nat_surf_terr_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS terr_nat_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sterr))::INTEGER AS terr_nat_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien IN (''232'',''233'',''239'')
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom)
		AND idnatmut in (1,2,4) -- on ne prend en compte que les ventes
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
		AND segmtab is null
		AND sterr>0		
        group by b.id, b.nom) e
    ON p.id=e.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS nonbati_indeterm_nb,
            round(percentile_disc(0.5) within group (order by sterr))::INTEGER AS nonbati_indeterm_surf_terr_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS nonbati_indeterm_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sterr))::INTEGER AS nonbati_indeterm_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien = ''20''
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom)
		AND idnatmut in (1,2,4) -- on ne prend en compte que les ventes
		AND valeurfonc!=0 AND valeurfonc!=1 AND valeurfonc is not null
		AND segmtab is null
		AND sterr>0		
        group by b.id, b.nom) f
    ON p.id=f.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS tab_fp_X0_nb,
            percentile_disc(0.5) within group (order by sterr)::INTEGER AS tab_fp_X0_surf_terr_median,
            percentile_disc(0.5) within group (order by valeurfonc)::INTEGER AS tab_fp_X0_prix_median,
            percentile_disc(0.5) within group (order by valeurfonc/sterr)::INTEGER AS tab_fp_X0_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''2%'' AND segmtab in (''3'',''4'')
        AND codtypproa LIKE ''X%'' --on prend en compte toutes les personnes physiques, même si ca n_est qu_un acquéreur parmi la liste des acquéreurs du bien
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom) 
		AND idnatmut in (1,2,4) -- on ne prend en compte que les ventes
		AND sterr>0
        group by b.id, b.nom) g
    ON p.id=g.id	
    ORDER BY p.nom';

EXECUTE 'COMMENT ON TABLE public.ind_foncier_'||dat||' IS ''Terrains non bâtis vendus par type : TAB (forte présomption), TAB potentiel (fiabilité faible), terrain artificialisé, terrain agricole, terrain naturel et forestier, terrain non bâti indéterminé''';
 
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.id IS ''Identifiant du zonage (code INSEE pour les communes ou code IRIS)''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.nom IS ''Nom du zonage''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.periode IS ''Période de traitement (bornes incluses)''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.nb_terrain IS ''Nombre de vente de terrains nus''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_fp_nb IS ''Nombre de vente de terrains non bâtis de type TAB (forte présomption)''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_fp_surf_terr_median IS ''Surface médiane de terrains non bâtis vendus de type TAB (forte présomption)''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_fp_prix_median IS ''Prix médian de terrains non bâtis vendus de type TAB (forte présomption)''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_fp_prix_euro_m2_median IS ''Prix médian au m² de terrains non bâtis vendus de type TAB (forte présomption)''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_pot_nb IS ''Nombre de vente de terrains non bâtis de type TAB potentiel (fiabilité faible)''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_pot_surf_terr_median IS ''Surface médiane de terrains non bâtis vendus de type TAB potentiel (fiabilité faible)''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_pot_prix_median IS ''Prix médian de terrains non bâtis vendus de type TAB potentiel (fiabilité faible)''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_pot_prix_euro_m2_median IS ''Prix médian au m² de terrains non bâtis vendus de type TAB potentiel (fiabilité faible)''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_artif_nb IS ''Nombre de vente de terrains non bâtis de type terrains artificialisés''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_artif_surf_terr_median IS ''Surface médiane de terrains non bâtis vendus de type terrains artificialisés''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_artif_prix_median IS ''Prix médian de terrains non bâtis vendus de type terrains artificialisés''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_artif_prix_euro_m2_median IS ''Prix médian au m² de terrains non bâtis vendus de type terrains artificialisés''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_agri_nb IS ''Nombre de vente de terrains non bâtis de type terrain agricole''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_agri_surf_terr_median IS ''Surface médiane de terrains non bâtis vendus de type terrain agricole''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_agri_prix_median IS ''Prix médian de terrains non bâtis vendus de type terrain agricole''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_agri_prix_euro_m2_median IS ''Prix médian au m² de terrains non bâtis vendus de type terrain agricole''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_nat_nb IS ''Nombre de vente de terrains non bâtis de type terrain naturel et forestier''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_nat_surf_terr_median IS ''Surface médiane de terrains non bâtis vendus de type terrain naturel et forestier''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_nat_prix_median IS ''Prix médian de terrains non bâtis vendus de type terrain naturel et forestier''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.terr_nat_prix_euro_m2_median IS ''Prix médian au m² de terrains non bâtis vendus de type terrain naturel et forestier''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.nonbati_indeterm_nb IS ''Nombre de vente de terrains non bâtis indéterminé''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.nonbati_indeterm_surf_terr_median IS ''Surface médiane de terrains non bâtis indéterminé''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.nonbati_indeterm_prix_median IS ''Prix médian de terrains non bâtis indéterminé''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.nonbati_indeterm_prix_euro_m2_median IS ''Prix médian au m² de terrains non bâtis indéterminé''';     
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_fp_X0_nb IS ''Nombre de vente de terrains non bâtis de type TAB (forte présomption, segmtab = 3 ou 4) acquis par des personnes physiques''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_fp_X0_surf_terr_median IS ''Surface médiane de terrains non bâtis vendus de type TAB (forte présomption) acquis par des personnes physiques''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_fp_X0_prix_median IS ''Prix médian de terrains non bâtis vendus de type TAB (forte présomption) acquis par des personnes physiques''';
EXECUTE 'COMMENT ON COLUMN public.ind_foncier_'||dat||'.tab_fp_X0_prix_euro_m2_median IS ''Prix médian au m² de terrains non bâtis vendus de type TAB (forte présomption) acquis par des personnes physiques'''; 
END;
$BODY$;

--ALTER FUNCTION public.__ind_foncier(text, text, text) OWNER TO postgres;
