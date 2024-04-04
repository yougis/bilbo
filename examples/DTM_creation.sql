SELECT
	a.proprio_ty,
	ARRAY_AGG(DISTINCT l_2014_n3)  AS classes_MOS,
	sum(a.values) as ha	
   FROM occupation_sol.faits_foncier_mos_formation_arboree_dafe_2014 a
     group by a.proprio_ty;