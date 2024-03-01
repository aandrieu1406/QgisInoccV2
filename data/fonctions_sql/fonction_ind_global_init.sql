-- FUNCTION: public.__ind_global(text, text, text, text)

-- DROP FUNCTION IF EXISTS public.__ind_global(text, text, text, text);

CREATE OR REPLACE FUNCTION public.__ind_global(
	an1 text,
	an2 text,
	dat text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    --VOLATILE PARALLEL UNSAFE
AS $BODY$

begin

EXECUTE 'DROP TABLE IF EXISTS public.ind_global_'||dat||' CASCADE';
EXECUTE 'CREATE TABLE public.ind_global_'||dat||' AS
SELECT 
    p.periode,
    p.id,
    p.nom,
    nb_adju,
    montant_adju,
    nb_echange,
    montant_echange,
    nb_expro,
    montant_expro,
    nb_vente,
    montant_vente,
    nb_ventes_foncier_nu,
    montant_ventes_foncier_nu,
    nb_ventes_bati,
    montant_ventes_bati,
    nb_total_ventes,
    montant_total_ventes,
    nb_appart,
    nb_maison,
    nb_depend,
    nb_activ,
	nb_bati_autre,
    nb_terrain,
    nb_vente_appart_seul,
    nb_vente_appart_deux,
    nb_vente_appart_multi,
    nb_appart_multi,
    act_tert_1_local_nb_vente,
    act_tert_1_local_surf_median,
    act_tert_1_local_prix_median,
    act_tert_1_local_prix_euro_m2_median,
    act_tert_locaux_mult_nb_vente,
    act_tert_locaux_mult_nb_locaux,
    act_tert_locaux_mult_prix_euro_m2_median,	
	p.geom
	
FROM
    (SELECT 
        id, nom, periode, geom
    FROM public.zonage) p
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) as nb_adju,
            round(sum(valeurfonc)) as montant_adju
        FROM dvf.mutation a , public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' and libnatmut=''Adjudication''
        and st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) a
    ON p.id=a.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) as nb_echange, 
            round(sum(valeurfonc)) as montant_echange
        FROM dvf.mutation a, public.zonage b 
        WHERE anneemut between '||an1||' AND '||an2||' and libnatmut=''Echange''
        and st_intersects(a.geomlocmut,b.geom) 
        group by b.id, b.nom) b
    ON p.id=b.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) as nb_expro, 
            round(sum(valeurfonc)) as montant_expro
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' and libnatmut=''Expropriation''
        and st_intersects(a.geomlocmut,b.geom) 
        group by b.id, b.nom) c
    ON p.id=c.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) as nb_vente, 
            round(sum(valeurfonc)) as montant_vente
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' and libnatmut LIKE ''Vente%''
        and st_intersects(a.geomlocmut,b.geom) 
        group by b.id, b.nom) d
    ON p.id=d.id
    LEFT JOIN
        (SELECT
            b.id,
            count(*) as nb_ventes_foncier_nu,
            round(sum(valeurfonc)) as montant_ventes_foncier_nu
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''2%''
        AND idnatmut IN (1,2,4)
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom)
        group by b.id, b.nom) e
    ON p.id=e.id
    LEFT JOIN
        (SELECT
            b.id,
            count(*) as nb_ventes_bati, 
            round(sum(valeurfonc)) as montant_ventes_bati
        FROM dvf.mutation a, public.zonage b  
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''1%''
        AND idnatmut IN (1,2,4)
        AND st_intersects(a.geomlocmut,b.geom) 
        group by b.id, b.nom) f
    ON p.id=f.id
    LEFT JOIN
        (SELECT
            b.id,
            count(*) as nb_total_ventes, 
            round(sum(valeurfonc)) as montant_total_ventes
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||'
        AND idnatmut IN (1,2,4)
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom) 
        group by b.id, b.nom) g
    ON p.id=g.id
    LEFT JOIN
        (SELECT 
            b.id,
            sum(nblocapt) as nb_appart
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''12%''
        AND idnatmut IN (1,2,4)
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) h
    ON p.id=h.id
    LEFT JOIN
        (SELECT 
            b.id,
            sum(nblocmai) as nb_maison
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''11%''
        AND idnatmut IN (1,2,4)
        AND st_intersects(a.geomlocmut,b.geom) 
        group by b.id, b.nom) i
    ON p.id=i.id
    LEFT JOIN
        (SELECT 
            b.id,
            sum(nblocdep) as nb_depend
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''13%''
        AND idnatmut IN (1,2,4)
        AND st_intersects(a.geomlocmut,b.geom) 
        group by b.id, b.nom) j
    ON p.id=j.id
    LEFT JOIN
        (SELECT 
            b.id,
            sum(nblocact) as nb_activ
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''14%''
        AND idnatmut IN (1,2,4)
        AND st_intersects(a.geomlocmut,b.geom) 
        group by b.id, b.nom) k
    ON p.id=k.id
	LEFT JOIN
        (SELECT 
			id, sum(nblocautre) as nb_bati_autre 
			FROM (SELECT b.id,
					case when codtypbien like ''10%'' and nblocmut = 0 then 1::numeric 
						 when codtypbien like ''15%'' then (nblocmut - nblocdep)::numeric 
						end as nblocautre
					FROM dvf.mutation a, public.zonage b
					WHERE anneemut between '||an1||' AND '||an2||' AND (codtypbien LIKE ''10%'' OR codtypbien LIKE ''15%'')
					AND idnatmut IN (1,2,4)
					AND st_intersects(a.geomlocmut,b.geom) 
					group by b.id, b.nom, codtypbien, nblocmut, nblocdep) w 
			group by id) l
	ON p.id=l.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) as nb_terrain
        FROM dvf.mutation a, public.zonage b 
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien LIKE ''2%''
        AND idnatmut IN (1,2,4)
        AND st_intersects(ST_Centroid(a.geomparmut),b.geom) 
        group by b.id, b.nom) m
    ON p.id=m.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) as nb_vente_appart_seul
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND (codtypbien LIKE ''121%'' OR codtypbien LIKE ''120%'')
        AND idnatmut IN (1,2,4) AND nblocapt=1
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) n
    ON p.id=n.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) as nb_vente_appart_deux
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND (codtypbien LIKE ''122%'' OR codtypbien LIKE ''120%'')
        AND idnatmut IN (1,2,4) AND nblocapt=2
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) o
    ON p.id=o.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) as nb_vente_appart_multi,
            sum(nblocapt) as nb_appart_multi
        FROM dvf.mutation a , public.zonage b  
        WHERE anneemut between '||an1||' AND '||an2||' AND (codtypbien LIKE ''123%'' OR codtypbien LIKE ''120%'')
        AND idnatmut IN (1,2,4) AND nblocapt>2
        AND st_intersects(a.geomlocmut,b.geom) 
        group by b.id, b.nom) q
    ON p.id=q.id
    LEFT JOIN
        (SELECT 
            b.id,
            sum(nblocact) AS act_tert_1_local_nb_vente,
            round(percentile_disc(0.5) within group (order by sbati))::INTEGER AS act_tert_1_local_surf_median,
            round(percentile_disc(0.5) within group (order by valeurfonc))::INTEGER AS act_tert_1_local_prix_median,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS act_tert_1_local_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien = ''143'' AND sbati>0 
        AND nblocact=1
        AND idnatmut IN (1,2,4)
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) r
    ON p.id=r.id
    LEFT JOIN
        (SELECT 
            b.id,
            count(*) AS act_tert_locaux_mult_nb_vente,
            sum(nblocact) AS act_tert_locaux_mult_nb_locaux,
            round(percentile_disc(0.5) within group (order by valeurfonc/sbati))::INTEGER AS act_tert_locaux_mult_prix_euro_m2_median
        FROM dvf.mutation a, public.zonage b
        WHERE anneemut between '||an1||' AND '||an2||' AND codtypbien = ''143'' AND sbati>0  -- avant : ffsbati>0
        AND nblocact>1
        AND idnatmut IN (1,2,4)
        AND st_intersects(a.geomlocmut,b.geom)
        group by b.id, b.nom) s
    ON p.id=s.id	
    ORDER BY p.nom';	

EXECUTE 'COMMENT ON TABLE public.ind_global_'||dat||' IS ''Nombre de transactions par type de mutation (adjudication, échange, expropriation, vente) et montant des transactions''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.id IS ''Identifiant du zonage (code INSEE pour les communes ou code IRIS)''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nom IS ''Nom du zonage''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.periode IS ''Période de traitement (bornes incluses)''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_adju IS ''Nombre d''''adjudications''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.montant_adju IS ''Montant total (€) pour les adjudication''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_echange IS ''Nombre d''''échanges''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.montant_echange IS ''Montant total (€) pour les échanges''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_expro IS ''Nombre d''''expropriations''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.montant_expro IS ''Montant total (€) pour les expropriations''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_vente IS ''Nombre total de ventes''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.montant_vente IS ''Montant total (€) pour les ventes''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_ventes_foncier_nu IS ''Nombre de ventes de foncier nu''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.montant_ventes_foncier_nu IS ''Montant total des ventes de foncier nu''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_ventes_bati IS ''nombre de vente de bâtis''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.montant_ventes_bati IS ''montant total de vente de bâtis''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_total_ventes IS ''nombre total de ventes''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.montant_total_ventes IS ''montant total des ventes''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_appart IS ''Nombre de ventes d''''appartements avec ou sans dépendance ou local d''''activité''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_maison IS ''Nombre de ventes de maisons avec ou sans dépendance ou local d''''activité'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_depend IS ''Nombre de dépendances vendues sans maison ou appartement ou local d''''activité'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_activ IS ''Nombre de locaux d''''activité vendus sans maison ou appartement''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_bati_autre IS ''Nombre de locaux "autres" : bâti indéterminé et bâti mixte''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_terrain IS ''Nombre de ventes de terrains nus'''; 
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_vente_appart_seul IS ''Nombre de ventes d''''appartements vendus à l''''unité''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_vente_appart_deux IS ''Nombre de ventes d''''appartements vendus par 2''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_vente_appart_multi IS ''Nombre de ventes d''''appartements vendus en lot (3 et plus)''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.nb_appart_multi IS ''nombre d''''appartements vendus dont la ventes comporte plusieurs appartements (3 et plus)''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.act_tert_1_local_nb_vente IS ''Nombre de locaux d''''activité tertiaire (services et commerces) vendus seuls''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.act_tert_1_local_surf_median IS ''surface médiane des locaux d''''activité tertiaire (services et commerces) vendus seuls''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.act_tert_1_local_prix_median IS ''prix médian des locaux d''''activité tertiaire (services et commerces) vendus seuls''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.act_tert_1_local_prix_euro_m2_median IS ''prix médian/m² des locaux d''''activité tertiaire (services et commerces) vendus seuls''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.act_tert_locaux_mult_nb_vente IS ''Nombre de vente de locaux d''''activité tertiaire (services et commerces) vendus en lots''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.act_tert_locaux_mult_nb_locaux IS ''Nombre de locaux d''''activité tertiaire (services et commerces) vendus en lots''';
EXECUTE 'COMMENT ON COLUMN public.ind_global_'||dat||'.act_tert_locaux_mult_prix_euro_m2_median IS ''prix médian/m² des locaux d''''activité tertiaire (services et commerces) vendus en lots''';   
END;
$BODY$;

--ALTER FUNCTION public.__ind_global(text, text, text) OWNER TO postgres;
