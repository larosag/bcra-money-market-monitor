SELECT 
      banco,
      tasa_pf_30d,
      promedio_bcra,
      ROUND (spread,2) AS spread

FROM tasas_bancos 

ORDER BY tasa_pf_30d DESC

LIMIT 10; 