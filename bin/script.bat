set OLD=C:\DATA\201906
set NEW=C:\DATA\201912

set FILE=�����������\UFL_PIPE_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=�����������\UFL_PIPE_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=�������\WTL_PIPE_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=�������\WTL_PIPE_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=�����\UFL_HPIP_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=�����\UFL_HPIP_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=���¼�\UFL_BPIP_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=���¼�\UFL_BPIP_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=õ���������\UFL_GPIP_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=õ���������\UFL_GPIP_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=��ż���\UFL_KPIP_LS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=��ż���\UFL_KPIP_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=�ϼ�����\SWL_PIPE_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=�ϼ�����\SWL_DEPT_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
