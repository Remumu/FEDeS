vcs=('11cb48' '6214e9' '6c71a0' 'b436b2' 'ee9e8c'
     '103959' '0e4a51' '2869ce' '7f04ca' 'e13805'
     'ed69ec' '51b7ef' '925e2b' '795a4c' '23dbec')

for vc in ${vcs[*]}; do
    mkdir $vc
    cp vs7.topology $vc
    mv $vc.trace $vc
done