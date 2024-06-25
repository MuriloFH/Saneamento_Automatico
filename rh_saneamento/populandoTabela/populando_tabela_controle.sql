--local_avaliacao_bloco_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'local_avaliacao_bloco_nulo'
where tipo_registro = '01-local-avaliacao'
and mensagem_erro like '%consta  com bloco vazio !%';

--km_distancia_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'km_distancia_nulo'
where tipo_registro = '02-Distancia'
and mensagem_erro like '%O total de KM não pode estar nulo%';

--dt_admissao_maior_dt_inicio_licenca
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_admissao_maior_dt_inicio_licenca'
where tipo_registro = '03-Matricula-licenca-premio'
and mensagem_erro like '%Não pode ser incluída uma configuração com data anterior à data de admissão da matrícula%';

--dt_inicial_dt_final_menor_dt_admissao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_inicial_dt_final_menor_dt_admissao'
where tipo_registro = '04-funcao-gratificada'
and mensagem_erro like '%Não pode ser incluída uma configuração com data anterior à data de admissão da matrícula%';

--dt_inicial_maior_dt_final_licenca
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_inicial_maior_dt_final_licenca'
where tipo_registro = '05-periodo-aquisitivo-licenca-premio'
and mensagem_erro like '%A data inicial não pode ser superior a data final.%';

--qtd_dias_direito_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'qtd_dias_direito_nulo'
where tipo_registro = '06-periodo-aquisitivo-licenca-premio'
and mensagem_erro like '%A quantidade de dias de direito deve ser informada.%';

--dt_inicial_e_final_licenca_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_inicial_e_final_licenca_nulo'
where tipo_registro = '07-periodo-aquisitivo-licenca-premio'
and mensagem_erro like '%A data inicial ou final não pode estar nula.%';

commit;
