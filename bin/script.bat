set OLD=C:\DATA\201906
set NEW=C:\DATA\201912

set FILE=광역상수관로\UFL_PIPE_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=광역상수관로\UFL_PIPE_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=상수관로\WTL_PIPE_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=상수관로\WTL_PIPE_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=열배관\UFL_HPIP_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=열배관\UFL_HPIP_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=전력선\UFL_BPIP_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=전력선\UFL_BPIP_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=천연가스배관\UFL_GPIP_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=천연가스배관\UFL_GPIP_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=통신선로\UFL_KPIP_LS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=통신선로\UFL_KPIP_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%

set FILE=하수관로\SWL_PIPE_LM
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
set FILE=하수관로\SWL_DEPT_PS
shpdiff -shp %OLD%\%FILE%.shp %NEW%\%FILE%.shp C:\temp\shpdiff_output\%FILE%
