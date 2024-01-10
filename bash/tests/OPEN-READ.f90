!subroutine chcopy(a,b,n)
! CHARACTER*1 A(1), B(1)
!
! DO 100 I = 1, N
!100     A(I) = B(I)
! return
! END


PROGRAM ReadFile
  !USE kinds,     ONLY : dp
  IMPLICIT NONE
  
  !INTEGER :: x = 2500000000
  INTEGER, PARAMETER :: DP = selected_real_kind(14,200)
  !INTEGER :: unit_number
  INTEGER  :: iuni
  CHARACTER(LEN=128) :: file_name, mount_point
  !character*1 :: mount_point(64), file_name(64)
  !CHARACTER(LEN=*) :: file_name
  INTEGER :: status
  CHARACTER(LEN=25) :: data_read
  INTEGER :: i
  INTEGER :: lenp

  INTEGER  :: igwx, igwx_, npwx, ik_, nbnd_
  REAL(DP) :: xk(3)
  INTEGER  :: ngw, nbnd, ispin, npol
  LOGICAL  :: gamma_only
  REAL(DP) :: scalef
                            

!  CALL get_environment_variable("MOUNT_POINT", mount_point(1))
!  write(6,*) '      MOUNT POINT:',mount_point(1) 

  ! Checks if MOUNT_POINT enviroment variables is set
  !lenp = ltrunc(mount_point,64)
!  lenp = LEN(mount_point)
!  WRITE(*,*) "mount point lenght = ", lenp
!  if (lenp > 0) then
!        call chcopy(mount_point(lenp),'wfc1',4)
!  else
        !lenp = ltrunc(mount_point,64)
!        lenp = 64
!        call chcopy(mount_point(1),'/beegfs/home/javier.garciablas/hercules/bash/tests/data/wfc1',64)
!  endif

!  call chcopy(file_name(1), mount_point, lenp)
!  lenp = lenp+1
!  call chcopy(file_name(lenp), '.dat', 4)

  
  ! Specify the file name
  !file_name = '/mnt/hercules/example.txt'
  !file_name = '/beegfs/home/javier.garciablas/hercules/bash/tests/data/wfc1'
  ! file_name = '/home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/hercules/bash/tests/data/wfc1'
  file_name = '/mnt/hercules/wfc1'

  ! Open the file for reading
  !OPEN(unit=unit_number, file=file_name, status='old', action='read', iostat=status)
  OPEN ( UNIT = iuni, FILE=TRIM(file_name)//'.dat', &
                          FORM='unformatted', STATUS = 'old', IOSTAT = status)
!                  FORM='unformatted'
  
  ! Check for errors in opening the file
  IF (status /= 0) THEN
    WRITE(*, *) 'Error opening file ', TRIM(file_name)
    STOP
  ELSE
    WRITE(*, *) 'File ', TRIM(file_name), ' opened'
  END IF

  WRITE(*,*) 'unit_number', iuni

  ! Read data from the file
  !READ(unit_number, *) ! Skip the first line (header)
  !READ(unit_number, *) data_read
  !READ(iuni, *) ! Skip the first line (header)
  !READ(iuni) data_read
  !WRITE(*, *) 'data_read: ', data_read
  READ (iuni) ik_, xk, ispin, gamma_only, scalef
  READ (iuni) ngw, igwx_, npol, nbnd_
  !READ (iuni) ik_
  !WRITE(*, *) 'ik_= ',ik_

  !READ (iuni) xk
  !WRITE(*, *) 'xk= ',xk

  !READ (iuni) ispin
  !WRITE(*, *) 'ispin= ',ispin
  

  !READ (iuni) gamma_only
  !WRITE(*, *) 'gamma_only= ',gamma_only

  !READ (iuni) scalef
  !WRITE(*, *) 'scalef= ',scalef
!  WRITE(*, *) 'LEN(scalef)=', LEN(scalef)

  !READ (iuni) ngw
  !WRITE(*, *) 'ngw= ',ngw

  !READ (iuni) igwx_
  !WRITE(*, *) 'igwx_=', igwx_

  !READ (iuni) npol
  !WRITE(*, *) 'npol= ',npol

  !READ (iuni) nbnd_
  !WRITE(*, *) 'nbnd_= ',nbnd_
  
  ! Close the file
  !CLOSE(unit_number)
  CLOSE(iuni, STATUS='DELETE')
  
  ! Display the read data
!  WRITE(*, *) 'Data read from file:'
  !DO i = 1, SIZE(data_read)
  WRITE(*, *) ik_, xk, ispin, gamma_only, scalef
  WRITE(*, *) ngw, igwx_, npol, nbnd_

  !END DO
  
END PROGRAM ReadFile

