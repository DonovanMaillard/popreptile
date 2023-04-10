
-------------------------------------------------final --POPReptile standard------------------------------------------
-- View: gn_monitoring.v_export_popreptile_standard

DROP  VIEW IF EXISTS gn_monitoring.v_export_popreptile_standard;

CREATE OR REPLACE VIEW gn_monitoring.v_export_popreptile_standard AS 
WITH site_protege AS (
   SELECT 
        csa_1.id_base_site,
        string_agg(DISTINCT ((la.area_name::text || '('::text) || bat.type_code::text) || ')'::text, ', '::text) AS sites_proteges
   FROM ref_geo.l_areas la
   JOIN ref_geo.bib_areas_types bat ON la.id_type = bat.id_type
   JOIN gn_monitoring.cor_site_area csa_1 ON csa_1.id_area = la.id_area
   WHERE bat.type_code::text = ANY (ARRAY[
   		'ZNIEFF1'::character varying, 
   		'ZPS'::character varying, 
   		'ZCS'::character varying, 
   		'SIC'::character varying, 
   		'RNCFS'::character varying, 
   		'RNR'::character varying, 
   		'RNN'::character varying, 
   		'ZC'::character varying]::text[])
   GROUP BY csa_1.id_base_site)
SELECT DISTINCT
    o.uuid_observation,
    tsg.sites_group_name AS aire_etude,
    tsg.uuid_sites_group AS uuid_aire_etude,
    tsg.sites_group_description AS description_aire,
    NULLIF(REPLACE((tsg.data::json->'habitat_principal')::text,'"',''),'null') AS habitat_principal_aire,
    NULLIF(REPLACE((tsg.data::json->'expertise')::text,'"',''),'null') AS expertise_operateur,
    tsg.comments AS commentaire_aire,
    s.base_site_name AS nom_transect,
    st_astext(s.geom) AS wkt,
    st_x(st_centroid(s.geom_local)) AS x_centroid_l93,
    st_y(st_centroid(s.geom_local)) AS y_centroid_l93,
    alt.altitude_min,
    alt.altitude_max,
    dep.area_name AS departement,
    dep.area_code AS code_dep,
    com.area_name AS commune,
    sp.sites_proteges AS sites_proteges,
    NULLIF(REPLACE((sc.data::json->'methode_prospection')::text,'"',''),'null') AS methode_prospection,
    NULLIF(REPLACE((sc.data::json->'type_materiaux')::text,'"',''),'null') AS type_materiaux,
    NULLIF(REPLACE((sc.data::json->'nb_plaques')::text,'"',''),'null') AS nb_plaques,
    NULLIF(REPLACE((sc.data::json->'milieu_transect')::text,'"',''),'null') AS milieu_transect,
    NULLIF(REPLACE((sc.data::json->'milieu_bordier')::text,'"',''),'null') AS milieu_bordier,
    NULLIF(REPLACE((sc.data::json->'milieu_mosaique_vegetale')::text,'"',''),'null') AS milieu_mosaique,
    NULLIF(REPLACE((sc.data::json->'milieu_homogene')::text,'"',''),'null') AS milieu_homogene,
    NULLIF(REPLACE((sc.data::json->'milieu_anthropique')::text,'"',''),'null') AS milieu_anthropique,
    NULLIF(REPLACE((sc.data::json->'milieu_transect_autre')::text,'"',''),'null') AS milieu_anthropique_autre,
    NULLIF(REPLACE((sc.data::json->'microhabitat_favorable')::text,'"',''),'null') AS microhab_favorable,
    NULLIF(REPLACE((sc.data::json->'frequentation_humaine')::text,'"',''),'null') AS frequentation_humaine,
    NULLIF(REPLACE((sc.data::json->'comment')::text,'"',''),'null') AS commentaire_transect,
    v.id_dataset,
    d.dataset_name AS jeu_de_donnees,
    v.uuid_base_visit AS uuid_visite,
    v.visit_date_min AS date_visite,
    NULLIF(REPLACE((vc.data::json->'Heure_debut')::text,'"',''),'null') AS heure_debut,
    NULLIF(REPLACE((vc.data::json->'Heure_fin')::text,'"',''),'null') AS heure_fin,
    NULLIF(REPLACE((vc.data::json->'num_passage')::text,'"',''),'null') AS num_passage,
    obs.observers,
    obs.organismes_rattaches,
    NULLIF(REPLACE((vc.data::json->'meteo')::text,'"',''),'null') AS meteo,
    NULLIF(REPLACE((vc.data::json->'vent')::text,'"',''),'null') AS vent,
    v.comments AS commentaire_visite,
    o.cd_nom,
    NULLIF(REPLACE((oc.data::json->'presence')::text,'"',''),'null') AS presence_reptile,
    t.lb_nom AS nom_latin,
    t.nom_vern AS nom_francais,
    NULLIF(REPLACE((oc.data::json->'abondance')::text,'"',''),'null') AS abondance,
    NULLIF(REPLACE((oc.data::json->'type_denombrement')::text,'"',''),'null') AS type_denbr,
    NULLIF(REPLACE((oc.data::json->'nombre_compte')::text,'"',''),'null') AS nombre_compte,
	NULLIF(REPLACE((oc.data::json->'nombre_estime_min')::text,'"',''),'null') AS nombre_estime_min,
	NULLIF(REPLACE((oc.data::json->'nombre_estime_max')::text,'"',''),'null') AS nombre_estime_max,
    NULLIF(REPLACE((oc.data::json->'stade_vie')::text,'"',''),'null') AS stade_vie,
    o.comments AS commentaire_obs
FROM gn_monitoring.t_observations o
LEFT JOIN gn_monitoring.t_observation_complements oc ON oc.id_observation = o.id_observation
LEFT JOIN gn_monitoring.t_base_visits v ON o.id_base_visit = v.id_base_visit
LEFT JOIN gn_monitoring.t_visit_complements vc ON v.id_base_visit = vc.id_base_visit
LEFT JOIN gn_monitoring.t_base_sites s ON s.id_base_site = v.id_base_site
LEFT JOIN gn_monitoring.t_site_complements sc ON sc.id_base_site = s.id_base_site
LEFT JOIN gn_monitoring.t_sites_groups tsg ON sc.id_sites_group = tsg.id_sites_group
LEFT JOIN gn_commons.t_modules m ON m.id_module = v.id_module
LEFT JOIN taxonomie.taxref t ON t.cd_nom = o.cd_nom
LEFT JOIN gn_monitoring.cor_site_area csa ON csa.id_base_site = s.id_base_site
LEFT JOIN gn_meta.t_datasets d ON d.id_dataset = v.id_dataset
LEFT JOIN ( SELECT la.area_name,
            csa_1.id_base_site
           FROM ref_geo.l_areas la
             JOIN ref_geo.bib_areas_types bat ON la.id_type = bat.id_type
             JOIN gn_monitoring.cor_site_area csa_1 ON csa_1.id_area = la.id_area
          WHERE bat.type_code::text = 'COM'::text) com ON s.id_base_site = com.id_base_site
LEFT JOIN ( SELECT la.area_name,
            la.area_code,
            csa_1.id_base_site
           FROM ref_geo.l_areas la
             JOIN ref_geo.bib_areas_types bat ON la.id_type = bat.id_type
             JOIN gn_monitoring.cor_site_area csa_1 ON csa_1.id_area = la.id_area
          WHERE bat.type_code::text = 'DEP'::text) dep ON s.id_base_site = dep.id_base_site
LEFT JOIN  site_protege sp ON s.id_base_site = sp.id_base_site
LEFT JOIN LATERAL ( SELECT array_agg(r.id_role) AS ids_observers,
            string_agg(concat(r.nom_role, ' ', r.prenom_role), ' ; '::text) AS observers,
            string_agg(DISTINCT org.nom_organisme::text, ', '::text) AS organismes_rattaches
           FROM gn_monitoring.cor_visit_observer cvo
             JOIN utilisateurs.t_roles r ON r.id_role = cvo.id_role
             LEFT JOIN utilisateurs.bib_organismes org ON org.id_organisme = r.id_organisme
          WHERE cvo.id_base_visit = v.id_base_visit) obs ON true
LEFT JOIN LATERAL ref_geo.fct_get_altitude_intersection(s.geom_local) alt(altitude_min, altitude_max) ON true
LEFT JOIN LATERAL ( SELECT ref_nomenclatures.get_nomenclature_label(json_array_elements(vc.data::json #> '{methode_de_prospection}'::text[])::text::integer, 'fr'::character varying) AS methodes) meth ON true
WHERE m.module_code::text = 'popreptile'::TEXT;
        

------------------------------------------------finale --POPReptile analyses------------------------------------------
-- View: gn_monitoring.v_export_popreptile_analyse
DROP VIEW IF EXISTS gn_monitoring.v_export_popreptile_analyses;

CREATE OR REPLACE VIEW gn_monitoring.v_export_popreptile_analyses AS 
WITH observations AS (
         SELECT o.id_base_visit,
            count(DISTINCT t.cd_ref) AS diversite,
            string_agg(DISTINCT t.lb_nom::text, ' ; '::text) AS taxons_latin,
            string_agg(DISTINCT t.nom_vern::text, ' ; '::text) AS taxons_fr,
            sum(NULLIF(REPLACE((oc.data::json->'nombre_compte')::text,'"',''),'null')::integer) + sum(NULLIF(REPLACE((oc.data::json->'nombre_estime_min')::text,'"',''),'null')::integer) AS count_min,
            sum(NULLIF(REPLACE((oc.data::json->'nombre_compte')::text,'"',''),'null')::integer) + sum(NULLIF(REPLACE((oc.data::json->'nombre_estime_max')::text,'"',''),'null')::integer) AS count_max
           FROM gn_monitoring.t_observations o
             LEFT JOIN taxonomie.taxref t ON o.cd_nom = t.cd_nom
             LEFT JOIN gn_monitoring.t_observation_complements oc ON oc.id_observation = o.id_observation
          GROUP BY o.id_base_visit
        ), 
    site_protege AS (
	SELECT 
        csa_1.id_base_site,
        string_agg(DISTINCT ((la.area_name::text || '('::text) || bat.type_code::text) || ')'::text, ', '::text) AS sites_proteges
   FROM ref_geo.l_areas la
   JOIN ref_geo.bib_areas_types bat ON la.id_type = bat.id_type
   JOIN gn_monitoring.cor_site_area csa_1 ON csa_1.id_area = la.id_area
   WHERE bat.type_code::text = ANY (ARRAY[
   		'ZNIEFF1'::character varying, 
   		'ZPS'::character varying, 
   		'ZCS'::character varying, 
   		'SIC'::character varying, 
   		'RNCFS'::character varying, 
   		'RNR'::character varying, 
   		'RNN'::character varying, 
   		'ZC'::character varying]::text[])
   	GROUP BY csa_1.id_base_site)
 SELECT DISTINCT tsg.sites_group_name AS aire_etude,
    tsg.uuid_sites_group AS uuid_aire_etude,
    tsg.sites_group_description AS description_aire,
    NULLIF(REPLACE((tsg.data::json->'habitat_principal')::text,'"',''),'null') AS habitat_principal_aire,
    NULLIF(REPLACE((tsg.data::json->'expertise')::text,'"',''),'null') AS expertise_operateur,
    tsg.comments AS commentaire_aire,
    s.base_site_name AS nom_transect,
    st_astext(s.geom) AS wkt,
    st_x(st_centroid(s.geom_local)) AS x_centroid_l93,
    st_y(st_centroid(s.geom_local)) AS y_centroid_l93,
    alt.altitude_min,
    alt.altitude_max,
    dep.area_name AS departement,
    dep.area_code AS code_dep,
    com.area_name AS commune,
    sp.sites_proteges AS sites_proteges,
    NULLIF(REPLACE((sc.data::json->'methode_prospection')::text,'"',''),'null') AS methode_prospection,
    NULLIF(REPLACE((sc.data::json->'type_materiaux')::text,'"',''),'null') AS type_materiaux,
    NULLIF(REPLACE((sc.data::json->'nb_plaques')::text,'"',''),'null') AS nb_plaques,
    NULLIF(REPLACE((sc.data::json->'milieu_transect')::text,'"',''),'null') AS milieu_transect,
    NULLIF(REPLACE((sc.data::json->'milieu_bordier')::text,'"',''),'null') AS milieu_bordier,
    NULLIF(REPLACE((sc.data::json->'milieu_mosaique_vegetale')::text,'"',''),'null') AS milieu_mosaique,
    NULLIF(REPLACE((sc.data::json->'milieu_homogene')::text,'"',''),'null') AS milieu_homogene,
    NULLIF(REPLACE((sc.data::json->'milieu_anthropique')::text,'"',''),'null') AS milieu_anthropique,
    NULLIF(REPLACE((sc.data::json->'milieu_transect_autre')::text,'"',''),'null') AS milieu_anthropique_autre,
    NULLIF(REPLACE((sc.data::json->'microhabitat_favorable')::text,'"',''),'null') AS microhab_favorable,
    NULLIF(REPLACE((sc.data::json->'frequentation_humaine')::text,'"',''),'null') AS frequentation_humaine,
    NULLIF(REPLACE((sc.data::json->'comment')::text,'"',''),'null') AS commentaire_transect,
    v.id_dataset,
    d.dataset_name AS jeu_de_donnees,
    v.uuid_base_visit AS uuid_visite,
    v.visit_date_min AS date_visite,
    NULLIF(REPLACE((vc.data::json->'Heure_debut')::text,'"',''),'null') AS heure_debut,
    NULLIF(REPLACE((vc.data::json->'Heure_fin')::text,'"',''),'null') AS heure_fin,
    NULLIF(REPLACE((vc.data::json->'num_passage')::text,'"',''),'null') AS num_passage,
    obs.observers,
    obs.organismes_rattaches,
    NULLIF(REPLACE((vc.data::json->'meteo')::text,'"',''),'null') AS meteo,
    NULLIF(REPLACE((vc.data::json->'vent')::text,'"',''),'null') AS vent,
    v.comments AS commentaire_visite,
    observations.diversite::integer AS diversite,
    observations.taxons_latin,
    observations.taxons_fr,
    observations.count_min AS abondance_total_min,
    observations.count_max AS abondance_total_max
   FROM gn_monitoring.t_base_visits v
     LEFT JOIN gn_monitoring.t_visit_complements vc ON v.id_base_visit = vc.id_base_visit
     LEFT JOIN gn_monitoring.t_base_sites s ON s.id_base_site = v.id_base_site
     LEFT JOIN gn_monitoring.t_site_complements sc ON sc.id_base_site = s.id_base_site
     LEFT JOIN gn_monitoring.t_sites_groups tsg ON sc.id_sites_group = tsg.id_sites_group
     LEFT JOIN gn_commons.t_modules m ON m.id_module = v.id_module
     LEFT JOIN gn_monitoring.cor_site_area csa ON csa.id_base_site = s.id_base_site
     LEFT JOIN observations ON observations.id_base_visit = v.id_base_visit
     LEFT JOIN gn_meta.t_datasets d ON d.id_dataset = v.id_dataset
     LEFT JOIN ( SELECT la.area_name,
            csa_1.id_base_site
           FROM ref_geo.l_areas la
             JOIN ref_geo.bib_areas_types bat ON la.id_type = bat.id_type
             JOIN gn_monitoring.cor_site_area csa_1 ON csa_1.id_area = la.id_area
          WHERE bat.type_code::text = 'COM'::text) com ON s.id_base_site = com.id_base_site
     LEFT JOIN ( SELECT la.area_name,
            la.area_code,
            csa_1.id_base_site
           FROM ref_geo.l_areas la
             JOIN ref_geo.bib_areas_types bat ON la.id_type = bat.id_type
             JOIN gn_monitoring.cor_site_area csa_1 ON csa_1.id_area = la.id_area
          WHERE bat.type_code::text = 'DEP'::text) dep ON s.id_base_site = dep.id_base_site
     LEFT JOIN site_protege sp ON s.id_base_site = sp.id_base_site
	 LEFT JOIN LATERAL ( SELECT array_agg(r.id_role) AS ids_observers,
            string_agg(concat(r.nom_role, ' ', r.prenom_role), ' ; '::text) AS observers,
            string_agg(DISTINCT org.nom_organisme::text, ', '::text) AS organismes_rattaches
           FROM gn_monitoring.cor_visit_observer cvo
             JOIN utilisateurs.t_roles r ON r.id_role = cvo.id_role
             LEFT JOIN utilisateurs.bib_organismes org ON org.id_organisme = r.id_organisme
          WHERE cvo.id_base_visit = v.id_base_visit) obs ON true
     LEFT JOIN LATERAL ref_geo.fct_get_altitude_intersection(s.geom_local) alt(altitude_min, altitude_max) ON true
  WHERE m.module_code::text = 'popreptile'::TEXT;  


-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
