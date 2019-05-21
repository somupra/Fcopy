#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define ERROR(s) do{ \
                        printf("Usage: %s <source> <target>\n0\tappend\n1\toverwrite\n2\tdelete\n %s\n", argv[0], s); \
                        exit(-1);\
                    }while(0);

#define SIZE 4096

typedef struct{
    unsigned long start;
    unsigned long size;
}partition;

char source_name[100], target_name[100];


void* overwrite(void*arg);
void* append(void*arg);

int main(int argc, char*argv[]){
    /*checking the parameters*/

    if(argc!=4)ERROR("NOT ENOUGH PARAMETERS");
    int n = atoi(argv[3]);
    if(!(n==0||n==1||n==2))ERROR("INVALID COMMAND, PLEASE READ THE MANUAL");

    /*arguements are checked*/

    strcpy(source_name, argv[1]);
    strcpy(target_name, argv[2]);


    struct stat filedetails;
    struct flock sourcelock, targetlock;

    /*firstly checking that the source file exists*/
    if(access(source_name,F_OK)== -1)ERROR("Source file doesn't exist, program will exit\n")
    else{
        /*source file exists*/
        /*partitioning the source file in 4 parts*/

            partition part1, part2, part3, part4;
            stat(source_name,&filedetails);
            part1.start = 0;
            part1.size = filedetails.st_size/4;
            part2.start = part1.size;
            part2.size = part1.size;
            part3.start = part2.size + part2.start;
            part3.size = part2.size;
            part4.start = part3.size + part3.start;
            part4.size = filedetails.st_size - part4.start;
        if(n==1 ||(n==0 && (access(target_name,F_OK)== -1))){
            /*overwrite*/
            int fin, fout;
            fin = open(source_name, O_RDONLY);
            if(fin < 0)ERROR("An error occured while opening the source file\nprogram exiting...\n");
            
            fout = open(target_name, O_WRONLY|O_CREAT|O_TRUNC, 0666);
            if(fout < 0)ERROR("An error occured while opening/creating the target file\nprogram exiting...\n");
            
            /*firstly, we will lock the source file for reading*/
            memset (&sourcelock, 0, sizeof(sourcelock));
            sourcelock.l_type = F_RDLCK;
            fcntl (fin, F_SETLKW, &sourcelock);
            printf("source file locked for reading\n");

            /*locking the target file for writing*/
            memset (&targetlock, 0, sizeof(targetlock));
            targetlock.l_type = F_WRLCK;
            fcntl (fout, F_SETLKW, &targetlock);
            printf("target file locked for writing\n");



            /*defining 3 threads to share the load of main thread*/
            pthread_t t1, t2, t3;
            pthread_create(&t1, NULL, overwrite, &part2);
            pthread_create(&t2, NULL, overwrite, &part3);
            pthread_create(&t3, NULL, overwrite, &part4);

            /*overwriting 1st part of file with the main thread*/
            int i = 0, x;
            char data[SIZE];
            while(i < part1.size){
                x = read(fin, data, SIZE);
                write(fout, data, x);
                i+=x;
            }
            /*making main to wait for the threads to finish*/
            pthread_join(t1,NULL);
            pthread_join(t2,NULL);
            pthread_join(t3,NULL);

            printf("File overwritten successfully, press 'ENTER' to release the locks\n");
            getchar();
            
            /*release the locks*/
            sourcelock.l_type = F_UNLCK;
            fcntl (fin, F_SETLKW, &lock);
            targetlock.l_type = F_UNLCK;
            fcntl (fout, F_SETLKW, &lock);
            printf("locks released successfully..\n");

            /*terminate the program successfully*/
            close(fin);
            close(fout);
            printf("exiting...\n");
            exit(0);

        }
        else if(n==0 && (access(target_name,F_OK) != -1)){
            /*append*/
            int fin, fout;
            fin = open(source_name, O_RDONLY);
            if(fin < 0)ERROR("An error occured while opening the source file\nprogram exiting...\n");
            
            fout = open(target_name, O_WRONLY|O_CREAT|O_APPEND, 0666);
            if(fout < 0)ERROR("An error occured while opening/creating the target file\nprogram exiting...\n");
            
            /*firstly, we will lock the source file for reading*/
            memset (&sourcelock, 0, sizeof(sourcelock));
            sourcelock.l_type = F_RDLCK;
            fcntl (fin, F_SETLKW, &sourcelock);
            printf("source file locked for reading\n");

            /*locking the target file for writing*/
            memset (&targetlock, 0, sizeof(targetlock));
            targetlock.l_type = F_WRLCK;
            fcntl (fout, F_SETLKW, &targetlock);
            printf("target file locked for writing\n");

            /*defining 3 threads to share the load of main thread*/
            pthread_t t1, t2, t3;
            pthread_create(&t1, NULL, append, &part2);
            pthread_create(&t2, NULL, append, &part3);
            pthread_create(&t3, NULL, append, &part4);

            /*appending 1st part of file with the main thread*/
            int i = 0, x;
            char data[SIZE];
            while(i < part1.size){
                x = read(fin, data, SIZE);
                write(fout, data, x);
                i+=x;
            }
            /*making main to wait for the threads to finish*/
            pthread_join(t1,NULL);
            pthread_join(t2,NULL);
            pthread_join(t3,NULL);

            printf("File appended successfully, press 'ENTER' to release the locks\n");
            getchar();
            
            /*release the locks*/
            sourcelock.l_type = F_UNLCK;
            fcntl (fin, F_SETLKW, &lock);
            targetlock.l_type = F_UNLCK;
            fcntl (fout, F_SETLKW, &lock);
            printf("locks released successfully..\n");

            /*terminate the program successfully*/
            close(fin);
            close(fout);
            printf("exiting...\n");
            exit(0);

        }
        else if(n==2){
            /*delete*/
			if(access(target_name,F_OK)!=-1){
				/*target file exists*/
				if((remove(source_name) || remove(target_name)==0)){
					printf("Files deleted successfully\n");
					exit(0);
				}else ERROR("An error occured while deleting the files");
			}else{
				printf("Target file doesn't exists, deleting the source file\n");
				if(remove(source_name)==0){
					printf("Source file deleted successfully\n");
					exit(0);
				}else ERROR("An error occured while deleting the source file\n");
			}	
        }
    }
    return 0;
}

void*overwrite(void*arg){
    
    int fin, fout, x, i=0;
	partition *part;
	char data[SIZE];
	
	part = (partition*)arg;
	fin = open(source_name, O_RDONLY);
    fout = open(target_name, O_WRONLY);
	
    lseek(fin, part->start, SEEK_SET);
	lseek(fout, part->start, SEEK_SET);
        while(i < part->size){
                x = read(fin, data, SIZE);
                write(fout, data, x);
                i += x;
        }
}

void*append(void*arg){
    
    int fin, fout, x, i=0;
	partition *part;
	char data[SIZE];
	
	part = (partition*)arg;
	fin = open(source_name, O_RDONLY);
    fout = open(target_name, O_WRONLY);
	
    lseek(fin, part->start, SEEK_SET);
	lseek(fout, part->start, SEEK_CUR);
        while(i < part->size){
                x = read(fin, data, SIZE);
                write(fout, data, x);
                i += x;
        }
}


