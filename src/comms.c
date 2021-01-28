#include <string.h>
#include <stdlib.h>
#include "zmq.h"
#include "imss.h"
#include "comms.h"

//Free function provided to zmq_msg_init_data in order to free the buffer once sent.
void free_msg (void * data, void * hint) {free(data);}

//Method sending a data structure with dynamic memory allocation fields.
int32_t
send_dynamic_struct(void *  socket,
		    void *  data_struct,
		    int32_t data_type)
{
	//Buffer containing the structures' information.
	unsigned char * info_buffer;
	//Buffer size.
	int64_t msg_size;

	//Formalize the information to be sent.
	switch (data_type)
	{
		case IMSS_INFO:
		{
			imss_info * struct_ = (imss_info *) data_struct;

			//Calculate the total size of the buffer storing the structure.

			msg_size = sizeof(imss_info) + (LINE_LENGTH * struct_->num_storages);

			//Reserve the corresponding amount of memory for the previous buffer.

			info_buffer = (unsigned char *) malloc(msg_size * sizeof(char));

			//Control variables dealing with incomming memory management actions.

			unsigned char * offset_pt = info_buffer;

			//Copy the actual structure to the buffer.

			memcpy(offset_pt, struct_, sizeof(imss_info));

			offset_pt += sizeof(imss_info);

			//Copy the remaining dynamic fields into the buffer.

			for (int32_t i = 0; i < struct_->num_storages; i++)
			{
				memcpy(offset_pt, struct_->ips[i], LINE_LENGTH);

				offset_pt += LINE_LENGTH;
			}

			break;
		}

		case DATASET_INFO:
		{
			dataset_info * struct_ = (dataset_info *) data_struct;

			//Calculate the total size of the buffer storing the structure.

			msg_size = sizeof(dataset_info);

			//If the dataset is a LOCAL one, the list of position characters must be added.

			if (!strcmp(struct_->policy, "LOCAL"))

				 msg_size += (struct_->num_data_elem * sizeof(uint16_t));

			//Reserve the corresponding amount of memory for the previous buffer.

			info_buffer = (unsigned char *) malloc(msg_size * sizeof(char));

			//Serialize the provided message into the buffer.

			unsigned char * offset_pt = info_buffer;

			//Copy the actual structure to the buffer.

			memcpy(offset_pt, struct_, sizeof(dataset_info));

			//Copy the remaining 'data_locations' field if the dataset is a LOCAL one.

			if (!strcmp(struct_->policy, "LOCAL"))
			{
				offset_pt += sizeof(dataset_info);

				memcpy(offset_pt, struct_->data_locations, (struct_->num_data_elem * sizeof(uint16_t)));
			}

			break;
		}
	}

	//Send the buffer.

	zmq_msg_t buffer_msg;

	zmq_msg_init_data(&buffer_msg, info_buffer, msg_size, free_msg, NULL);

	if (zmq_msg_send (&buffer_msg, socket, 0) != msg_size)
	{
		perror("ERRIMSS_SENDDYNAMSTRUCT");
		return -1;
	}

	zmq_msg_close(&buffer_msg);

	return 0;
}

//Method retrieving a serialized dynamic data structure.
int32_t
recv_dynamic_struct(void *  socket,
		    void *  data_struct,
		    int32_t data_type)
{
	//Create a ZeroMQ massage container receiving the structure.

	zmq_msg_t msg_struct;

	if (zmq_msg_init(&msg_struct) != 0)
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_INIT");
		return -1;
	}

	if (zmq_msg_recv(&msg_struct, socket, 0) == -1)
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		return -1;
	}

	//Actual message content plus message size.

	unsigned char * msg_data = (unsigned char *) zmq_msg_data(&msg_struct);

	//Formalize the received information.

	switch (data_type)
	{
		case IMSS_INFO:
		{
			imss_info * struct_ = (imss_info *) data_struct;

			//Copy the actual structure into the one provided through reference.

			memcpy(struct_, msg_data, sizeof(imss_info));

			if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", struct_->uri_, 22))
			{
				zmq_msg_close(&msg_struct);
				return 0;
			}

			msg_data += sizeof(imss_info);

			//Copy the dynamic fields into the structure.

			struct_->ips = (char **) malloc(struct_->num_storages * sizeof(char *));

			for (int32_t i = 0; i < struct_->num_storages; i++)
			{
				struct_->ips[i] = (char *) malloc(LINE_LENGTH * sizeof(char));

				memcpy(struct_->ips[i], msg_data, LINE_LENGTH);

				msg_data += LINE_LENGTH;
			}

			break;
		}

		case DATASET_INFO:
		{
			dataset_info * struct_ = (dataset_info *) data_struct;

			//Copy the actual structure into the one provided through reference.

			memcpy(struct_, msg_data, sizeof(dataset_info));

			if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", struct_->uri_, 22))
			{
				zmq_msg_close(&msg_struct);
				return 0;
			}

			//If the size of the message received was bigger than sizeof(dataset_info), something more came with it.

			if (zmq_msg_size(&msg_struct) > sizeof(dataset_info))
			{
				msg_data += sizeof(dataset_info);

				//Copy the remaining 'data_locations' field into the structure.

				struct_->data_locations = (uint16_t *) malloc(struct_->num_data_elem * sizeof(uint16_t));

				memcpy(struct_->data_locations, msg_data, (struct_->num_data_elem * sizeof(uint16_t)));
			}

			break;
		}
	}

	zmq_msg_close(&msg_struct);

	return 1;
}
