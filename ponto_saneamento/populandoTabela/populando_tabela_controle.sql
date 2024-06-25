--descricao_repetida
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_repetida'
where tipo_registro = '1-Horario ponto'
and mensagem_ajuda like '%Busca as descrições repetidas no horário ponto%';

--descricao_repetida_turmas
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_repetida_turmas'
where tipo_registro = '2-Turma'
and mensagem_ajuda like '%Busca as descrições repetidas na turma%';

--descricao_motivo_altera_ponto_maior_30_caracter
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_motivo_altera_ponto_maior_30_caracter'
where tipo_registro = '3-Alteracao Ponto'
and mensagem_ajuda like '%Verifica a descrição do motivo de alteração do ponto se contém mais que 30 caracteres%';

--origem_marcacao_invalida
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'origem_marcacao_invalida'
where tipo_registro = '4-Marcacoes'
and mensagem_ajuda like '%Alterar origem da marcação%';

--descricao_duplicada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_duplicada'
where tipo_registro = '5-Ocorrencias Ponto'
and mensagem_ajuda like '%Já existe uma ocorrência de ponto com a descrição informada%';

--dt_inicio_e_fim_nula
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_inicio_e_fim_nula'
where tipo_registro = '6-permuta'
and mensagem_ajuda like '%As datas não podem ser nulas%';

commit;
